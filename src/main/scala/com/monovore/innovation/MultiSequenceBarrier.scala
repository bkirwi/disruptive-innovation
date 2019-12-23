package com.monovore.innovation

import com.lmax.disruptor.Sequence
import com.lmax.{disruptor => lmax}

class MultiSequenceBarrier(barriers: Array[lmax.SequenceBarrier]) extends lmax.SequenceBarrier {

  override def waitFor(sequence: Long): Long = {
    var minWaited = sequence
    for (barrier <- barriers) {
      minWaited = Math.min(minWaited, barrier.waitFor(sequence))
    }
    minWaited
  }

  override def getCursor: Long = {
    var cursor = Long.MaxValue
    for (barrier <- barriers) {
      cursor = Math.min(cursor, barrier.getCursor)
    }
    cursor
  }

  override def isAlerted: Boolean = barriers.exists(_.isAlerted)

  override def alert(): Unit = barriers.foreach(_.alert())

  override def clearAlert(): Unit = barriers.foreach(_.clearAlert())

  override def checkAlert(): Unit = barriers.foreach(_.checkAlert())
}

class ForwardingSequenceBarrier(
  buffer: lmax.RingBuffer[event.Event.LongEvent],
  myBarrier: lmax.SequenceBarrier,
  otherBarrier: lmax.SequenceBarrier
) extends lmax.SequenceBarrier {

  val completed = new Sequence(buffer.getCursor)

  override def waitFor(sequence: Long): Long = {
    val completedOffset = completed.get()
    if (sequence >= completedOffset) sequence
    else {
      val highWater = myBarrier.waitFor(sequence)
      val otherBuffer = buffer.get(highWater).value

    }
  }

  override def getCursor: Long = ???

  override def isAlerted: Boolean = ???

  override def alert(): Unit = ???

  override def clearAlert(): Unit = ???

  override def checkAlert(): Unit = ???
}
