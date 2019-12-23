package com.monovore.innovation

import cats.implicits._
import cats.{Applicative, Monad}
import com.lmax.{disruptor => lmax}
import cats.free.{Free, FreeApplicative}
import com.monovore.innovation.event.Event

class Key[A] {
  def read: Key.F[A] = FreeApplicative.lift(this)
}

object Key {
  type F[A] = FreeApplicative[Key, A]
  def pure[A](a: A): F[A] = FreeApplicative.pure(a)
}

sealed trait Action[A]
case class Allocate[A, M](event: Event[A] { type Mutable = M }, size: Int) extends Action[Key[lmax.RingBuffer[M]]]
case class Register[A](source: Key.F[lmax.EventProcessor]) extends Action[Key[lmax.Sequence]]
case class AddPublisher[A](buffer: Key[lmax.RingBuffer[A]]) extends Action[Unit]

object Action {
  type F[A] = Free[Action, A]

  def allocate[A](size: Int)(implicit event: Event[A]): F[Key[lmax.RingBuffer[event.Mutable]]] =
    Free.liftF(Allocate(event, size))

  def register(processor: Key.F[lmax.EventProcessor]): F[Key[lmax.Sequence]] =
    Free.liftF(Register(processor))

  def addPublisher[A](buffer: Key[lmax.RingBuffer[A]]): F[Unit] =
    Free.liftF[Action, Unit](AddPublisher(buffer))
}

case class Disruptor[A](free: Action.F[A])

object Disruptor {
  implicit val monadDisruptor: Monad[Disruptor] = ???
}

object RingBuffer {

  trait EventAndBuffer[A] {
    type Mutable
    val event: Event[A] { type Mutable = EventAndBuffer.this.Mutable }
    val bufferKey: Key[lmax.RingBuffer[Mutable]]
  }

  object EventAndBuffer {
    def apply[A](event0: Event[A])(bufferKey0: Key[lmax.RingBuffer[event0.Mutable]]): EventAndBuffer[A] =
      new EventAndBuffer[A] {
        override type Mutable = event0.Mutable
        val event = event0
        override val bufferKey: Key[lmax.RingBuffer[event0.Mutable]] = bufferKey0
      }
  }

  def allocate[A](size: Int)(implicit event: Event[A]): Disruptor[RingBuffer[A]] = {
    Disruptor(Action.allocate(size)(event))
      .map(bufferKey0 => new RingBuffer(EventAndBuffer(event)(bufferKey0)))
  }
}

class RingBuffer[A](private[RingBuffer] val eventAndBuffer: RingBuffer.EventAndBuffer[A]) {

  import eventAndBuffer._

  private[this] case class Junk[B](sequences: List[lmax.Sequence], provider: lmax.DataProvider[B])

  private[this] abstract class Sinking[B] {
    private[RingBuffer] def publish(b: B): Long
  }

  class Sink[B] private[RingBuffer](private[RingBuffer] val to: Key.F[Sinking[B]])

  def output: Handler[A] =
    new Handler(bufferKey.read.map(buffer =>
      Junk(Nil, idx => event.translateFrom(buffer.get(idx)))
    ))

  def input: Sink[A] =
    new Sink[A](bufferKey.read.map(buffer =>
      new Sinking[A] {
        override private[RingBuffer] def publish(a: A) = {
          val next = buffer.next()
          try {
            val mut = buffer.get(next)
            event.translateTo(a, mut)
            next
          } finally {
            buffer.publish(next)
          }
        }
      }
    ))

  class Handler[B] private[RingBuffer](private[RingBuffer] val from: Key.F[Junk[B]]) {

    private[this] def process(eventHandler: Key.F[lmax.EventHandler[B]]): Disruptor[Key[lmax.Sequence]] =
      Disruptor(Action.register(
        (bufferKey.read, from, eventHandler)
          .mapN { case (buffer, Junk(sequences, provider), eventHandler)  =>
            new lmax.BatchEventProcessor[B](
              provider,
              buffer.newBarrier(sequences: _*),
              eventHandler
            )
          }
      ))

    def run(implicit evidence: B =:= Unit): Disruptor[Handler[Unit]] =
      process(Key.pure((e, _, _) => evidence(e)))
        .map(sequenceKey =>
          new Handler(sequenceKey.read.map(sequence => Junk(List(sequence), _ => ())))
        )


    /**
     * Here's an interesting one! Let's say we want to consume from buffer A and publish some derivative data
     * to buffer B. We then have some consumer of A that wants to block on some consumer of B!
     *
     *
     *
     * @param other
     * @return
     */
    def publishTo(other: RingBuffer[B]): Disruptor[other.Handler[Unit] => Handler[Unit]] = {
      for {
        translationBuffer <- RingBuffer.allocate[Long](128) // FIXME: preserve size!
        sequenceKey <- process {

          (other.input.to, translationBuffer.input.to).mapN((otherBuffer, offsets) =>
            (e, _, _) => {
              val offset = otherBuffer.publish(e)
              offsets.publish(offset)
            }
          )
        }
        _ <- Disruptor(Action.addPublisher(other.eventAndBuffer.bufferKey))
        _ <- Disruptor(Action.addPublisher(translationBuffer.eventAndBuffer.bufferKey))
      } yield { otherHandler: other.Handler[Unit] =>


        /*
        So, what's the idea here? We're building a lookup table, a la [0 0 0 2 2 5 5 5].
        When people retrieve the
         */
        new Handler[Unit](
          this.from.map { case (sequences, _) =>
          }
        )
      }
    }
  }

  object Handler {
    implicit val applicativeHandler: Applicative[Handler] = new Applicative[Handler] {

      override def pure[B](x: B): Handler[B] =
        new Handler[B](FreeApplicative.pure(Junk(List.empty, _ => x)))

      override def ap[B, C](ff: Handler[B => C])(fa: Handler[B]): Handler[C] =
        new Handler((ff.from, fa.from).mapN { (ff, fa) =>
          Junk[C](
            ff.sequences ++ fa.sequences,
            x => ff.provider.get(x).apply(fa.provider.get(x))
          )
        })
    }
  }
}



object C {

  val disruptor: Disruptor[Unit] = for {
    buffer <- RingBuffer.allocate[Long](size = 256)
    journal <- buffer.output.map(x => println(s"journalling $x")).run
    replicate <- buffer.output.map(x => println(s"replicating $x")).run
    business <-
      (buffer.output <* journal <* replicate)
        .map(x => println(s"businessing $x")).run
  } yield ()

  /*

  for {
    buffer <- ...
    (republished, newBuffer) <- buffer.output.producer.flatMap(x => <xs>).republish(size = 256)

  }

   */

}