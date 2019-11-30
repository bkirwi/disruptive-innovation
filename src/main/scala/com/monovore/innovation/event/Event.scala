package com.monovore.innovation.event

import com.lmax.{disruptor => lmax}

trait Event[A] { event =>
  type Mutable

  def newInstance(): Mutable
  def translateTo(a: A, mutable: Mutable): Unit
  def translateFrom(mutable: Mutable): A

  object translator extends lmax.EventTranslatorOneArg[Mutable, A]  {
    override def translateTo(mutable: Mutable, sequence: Long, a: A): Unit =
      event.translateTo(a, mutable)
  }
}

object Event {

  class LongEvent(var value: Long)

  implicit val longEvent: Event[Long] = new Event[Long] {
    override type Mutable = LongEvent
    override def newInstance(): Mutable = new LongEvent(0L)
    override def translateTo(a: Long, mutable: Mutable): Unit = mutable.value = a
    override def translateFrom(mutable: Mutable): Long = mutable.value
  }

  class NullableEvent[A >: Null](var value: A)

  implicit def nullableEvent[A >: Null]: Event[A] = new Event[A] {
    override type Mutable = NullableEvent[A]
    override def newInstance(): NullableEvent[A] = new NullableEvent(null)
    override def translateTo(a: A, mutable: NullableEvent[A]): Unit = mutable.value = a
    override def translateFrom(mutable: NullableEvent[A]): A = mutable.value
  }
}