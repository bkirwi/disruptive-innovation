package com.monovore.innovation

import cats.implicits._
import cats.{Applicative, Monad}
import com.lmax.{disruptor => lmax}
import cats.free.{Free, FreeApplicative}
import com.monovore.innovation.event.Event

case class Junk[B](sequences: List[lmax.Sequence], provider: lmax.DataProvider[B])

class Key[A] {
  def read: Key.F[A] = FreeApplicative.lift(this)
}

object Key {
  type F[A] = FreeApplicative[Key, A]
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
  def allocate[A](size: Int)(implicit event: Event[A]): Disruptor[RingBuffer[A]] = {
    Disruptor(Action.allocate(size)(event))
      .map(bufferKey =>
        new RingBuffer[A](
          bufferKey.read.widen[lmax.RingBuffer[_]],
          bufferKey.read.map(buffer =>
            idx => event.translateFrom(buffer.get(idx))
          )
        )
      )
  }
}

class RingBuffer[A](bufferKey: Key.F[lmax.RingBuffer[_]], providerKey: Key.F[lmax.DataProvider[A]]) {

  def output: Handler[A] = new Handler(providerKey.map(provider => Junk(Nil, provider)))

  class Handler[B] private[RingBuffer](private[RingBuffer] val from: Key.F[Junk[B]]) {

    def run(implicit evidence: B =:= Unit): Disruptor[Handler[Unit]] = {

      val processor: Key.F[lmax.EventProcessor] =
        (bufferKey, from)
          .mapN { case (buffer, Junk(sequences, provider))  =>
            new lmax.BatchEventProcessor[B](
              provider,
              buffer.newBarrier(sequences: _*),
              (e, _, _) => evidence(e)
            )
          }

      Disruptor(Action.register(processor))
        .map(sequenceKey =>
          new Handler(sequenceKey.read.map(sequence => Junk(List(sequence), _ => ())))
        )
    }
//
//    def republish(implicit event: Event[B]): Disruptor[Handler[B]] = {
//      for {
//        downstream <- RingBuffer.allocate(event)
//        processor = new BatchEventProcessor[B](
//          provider,
//          RingBuffer.this.buffer.newBarrier(sequences: _*),
//          (e, _, _) => {
//            downstream.buffer.publishEvent(downstream.event.translator, e)
//          }
//        )
//        _ <- Disruptor(Free.liftF(Register(processor)))
//      } yield Handler(
//        List(processor.getSequence),
//        x => downstream.event.translateFrom(downstream.buffer.get(x))
//      )
//    }
//
//    def publishTo(buffer: RingBuffer[B]): Disruptor[Unit] = {
//
//      val processor = new BatchEventProcessor[B](
//        provider,
//        RingBuffer.this.buffer.newBarrier(sequences: _*),
//        (e, _, _) => {
//          buffer.buffer.publishEvent(buffer.event.translator, e)
//        }
//      )
//
//      Disruptor(Free.liftF(Register(processor)))
//    }
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

//      override def map[A, B](fa: Handler[A])(f: A => B): Handler[B] =
//        Handler(fa.sequences, x => f(fa.provider.get(x)))
//
//      override def productR[A, B](fa: Handler[A])(fb: Handler[B]): Handler[B] =
//        Handler(fa.sequences ++ fb.sequences, fb.provider)
//
//      override def productL[A, B](fa: Handler[A])(fb: Handler[B]): Handler[A] =
//        Handler(fa.sequences ++ fb.sequences, fa.provider)
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