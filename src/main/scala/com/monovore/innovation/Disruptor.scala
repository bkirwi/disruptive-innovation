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

  case class Junk[B](sequences: List[lmax.Sequence], provider: lmax.DataProvider[B])

  def output: Handler[A] =
    new Handler(bufferKey.read.map(buffer =>
      Junk(Nil, idx => event.translateFrom(buffer.get(idx)))
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

    def publishTo(other: RingBuffer[B]): Disruptor[Handler[Unit]] = {
      for {
        sequenceKey <- process {
          import other.eventAndBuffer._
          bufferKey.read.map(buffer =>
            (e, _, _) => buffer.publishEvent(event.translator, e)
          )
        }
        _ <- Disruptor(Action.addPublisher(bufferKey))
      } yield new Handler(sequenceKey.read.map(sequence => Junk(List(sequence), _ => ())))
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
}