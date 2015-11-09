package com.seven.util

import scala.reflect.ClassTag
import scala.collection.mutable.Map
import sun.security.util.Length
import java.util.Random
import scala.util.control.BreakControl

/**
 * a priority queue support enQueue enQueueLimit deQueue update value ,key and position relation in heap
 * a max heap or a min heap depends on ord
 * thread unsafe
 * @author seven
 */
class PriorityQueue[T: ClassTag](length: Int = 16, ord: Ordering[T]) {
  private var _size = 0
  private var _length = length //default array size
  //an Ordering that provide compare method of element
  private var _ord: Ordering[T] = ord
  //heap that hold all elements
  private var _arr: Array[T] = new Array[T](_length)
  //tragedy to expend heap when need more space to hold element
  private var _expendFactor = 0.5
  //wheather to  keep the key-value relation of the element and its posion in heap
  private var _holdKp: Boolean = false
  //key-value relation of the element and its posion in heap
  private var _kp: Map[String, Int] = _
  //method to get key from element
  private var _getKf: (T) => String = _

  def this(length: Int, ord: Ordering[T], holdKp: Boolean, f: (T) => String) = {
    this(length, ord)
    this._holdKp = holdKp
    if (_holdKp) {
      _kp = Map[String, Int]()
      _getKf = f
    }
  }

  def this(length: Int, ord: Ordering[T], holdKp: Boolean, f: (T) => String, extendFactor: Float) = {
    this(length, ord, holdKp, f)
    this._expendFactor = extendFactor
  }

  private def moveUp(i: Int, t: T): Unit = {
    var c = i
    var p = (c - 1) >>> 1
    while (c > 0 && _ord.compare(_arr(p), t) > 0) {
      _arr(c) = _arr(p)

      if (_holdKp)
        _kp += (_getKf(_arr(c)) -> c)

      c = p
      p = (c - 1) >>> 1
    }
    _arr(c) = t

    if (_holdKp)
      _kp += (_getKf(t) -> c)
  }

  private def findMinIndex(c: Int, t: T): Int = {
    var minIndex = c
    var min = t

    val l = (c << 1) + 1
    if (l < _size)
      if (_ord.compare(t, _arr(l)) > 0) {
        minIndex = l
        min = _arr(l)
      }

    val r = (c << 1) + 2
    if (r < _size)
      if (_ord.compare(min, _arr(r)) > 0) {
        minIndex = r
      }

    minIndex
  }

  private def moveDown(i: Int, t: T): Unit = {
    var c = i
    var minIndex = findMinIndex(c, t)
    while (minIndex != c) {
      _arr(c) = _arr(minIndex)

      if (_holdKp)
        _kp += (_getKf(_arr(c)) -> c)

      c = minIndex
      minIndex = findMinIndex(c, t)
    }
    _arr(minIndex) = t

    if (_holdKp)
      _kp += (_getKf(t) -> minIndex)
  }

  /**
   * when queue is full only element that is greater than header can be added
   * this can be used to sole topN problem when memory is limited
   */
  def enQueueLimit(t: T): PriorityQueue[T] = {
    if (_size < _length) {
      var i = _size
      moveUp(i, t)
      _size += 1
    } else if (_ord.compare(t, _arr(0)) > 0) {
      moveDown(0, t)
    }

    this
  }

  /**
   * add element to queue,first add to end of heap and try to move up
   */
  def enQueue(t: T): PriorityQueue[T] = {
    if (_size < _length) {
      var i = _size
      moveUp(i, t)
      _size += 1
    } else {
      //TODO  expend space and add
    }

    this
  }

  /**
   * delete element from queue, first save first element in heap then adjust heap
   */
  def deQueue(): Option[T] = {
    if (_size > 0) {
      val ret = _arr(0)
      _size -= 1
      val t = _arr(_size)

      if (_holdKp)
        _kp.remove(_getKf(ret))

      if (_size > 0)
        moveDown(0, t)
      //TODO free space 
      Some(ret)
    } else {
      None
    }
  }

  /**
   * update value of a element in position i in heap then ajust heap
   */
  def update(i: Int, t: T): PriorityQueue[T] = {
    var cmp = _ord.compare(t, _arr(i))
    if (cmp < 0)
      moveUp(i, t)
    else if (cmp > 0) {
      moveDown(i, t)
    }

    this
  }

  def update(k: String, t: T): PriorityQueue[T] = {
    val i = getPos(k)
    i match {
      case None => this
      case _    => update(i.get, t)
    }
  }

  def get(i: Int): Option[T] = if (i < _size) Some(_arr(i)) else None

  def get(k: String): Option[T] = {
    val i = getPos(k)
    i match {
      case None => None
      case _    => get(i.get)
    }
  }

  def contains(k: String): Boolean = {
    _kp.contains(k)
  }

  def contains(i: Int): Boolean = if (i < _size) true else false

  def isEmpty: Boolean = if (_size == 0) true else false

  def size: Int = _size

  def getOrd: Ordering[T] = ord

  def getPos(k: String): Option[Int] = {
    try {
      val pos = _kp(k)
      Some(pos)
    } catch {
      case ex: NoSuchElementException => None
    }
  }

  def getHeap(): Array[T] = _arr

  def getKPos(): Option[Map[String, Int]] = if (_holdKp) Some(_kp) else None

  def printHeap(): Unit = {
    _arr.foreach(x => { print(x); print(" ") })
    println
  }

  def printKPos(): Unit = {
    if (_holdKp) {
      _kp.foreach(x => { print(x); print(" ") })
      println
    } else {
      println("no key posion relationship hold")
    }
  }
}

object PriorityQueue {

  def check(testCnt: Int, opCnt: Int, initSizeRange: Int): Unit = {
    val rand = new Random
    for (i <- 1 to testCnt) {
      val initSize = rand.nextInt(initSizeRange)
      val limitQueue = new PriorityQueue[(String, Int)](initSize, ord = Ordering.by[(String, Int), Int] { x => -x._2 }, true, x => x._1)
      for (j <- 1 to initSize) {
        val e = genElement()
        val arr = limitQueue.getHeap().clone()
        limitQueue.enQueueLimit(e)
        if (!checkHeap(limitQueue)) {
          println("Array size = " + initSize)
          arr.foreach(x => { print(x); print(" ") })
          println("Add " + e)
          throw new RuntimeException("error case found")
        }

      }

      var op = 0
      var updateIndex = 0
      for (j <- 1 to opCnt) {
        op = rand.nextInt(2)
        val arr = limitQueue.getHeap().clone()
        //delete
        if (op == 0) {
          limitQueue.deQueue()
          if (!checkHeap(limitQueue)) {
            println("Array")
            arr.foreach(x => { print(x); print(" ") })
            println("deQueue")
            throw new RuntimeException("error case found")
          }
        } else { //update
          val e = genElement()
          if (limitQueue.size > 0) {
            updateIndex = rand.nextInt(limitQueue.size)
            limitQueue.update(updateIndex, e)
            if (!checkHeap(limitQueue)) {
              println("Array")
              arr.foreach(x => { print(x); print(" ") })
              println("Update updateIndex=" + updateIndex + ", newValue=" + e)
              throw new RuntimeException("error case found")
            }
          }
        }
      }
    }

  }

  def genElement(): (String, Int) = {
    val rand = new Random
    ("" + ('a' + rand.nextInt(26)).toChar, rand.nextInt(100))
  }

  def checkHeap[T: ClassTag](queue: PriorityQueue[T]): Boolean = {
    val arr = queue.getHeap()
    val size = queue.size
    val ord = queue.getOrd
    if (size < 0)
      throw new RuntimeException("size can not be nagtive")

    if (size == 0)
      return true

    val maxParent = (size - 1) >>> 1
    var l = 0
    var r = 0
    for (i <- 0 to maxParent) {
      l = (i << 1) + 1
      r = (i << 1) + 2
      if (l < size && ord.compare(arr(i), arr(l)) > 0)
        return false
      if (r < size && ord.compare(arr(i), arr(r)) > 0)
        return false
    }

    true
  }

  def test(): Unit = {
    //    val limitQueue = new PriorityQueue[(String, Int)](10, Ordering.by[(String, Int), Int] { x => x._2 })
    //asc
    //    val limitQueue = new PriorityQueue[(String, Int)](ord = Ordering.by[(String, Int), Int] { x => x._2 })
    //desc
    //     val limitQueue = new PriorityQueue[(String, Int)](ord = Ordering.by[(String, Int), Int] { x => -x._2 })
    val limitQueue = new PriorityQueue[(String, Int)](16, ord = Ordering.by[(String, Int), Int] { x => -x._2 }, true, x => x._1)
    //    val limitQueue = new LimitPriorityQueue[(String, Int)](10, Ordering[Int].on { x => x._2 })

    limitQueue.enQueueLimit(("y", 10))
    limitQueue.enQueueLimit(("m", 5))
    limitQueue.enQueueLimit(("s", 0))
    limitQueue.enQueueLimit(("t", 1))
    limitQueue.enQueueLimit(("w", 1))
    limitQueue.enQueueLimit(("u", 1))
    limitQueue.printHeap()
    limitQueue.printKPos()
    limitQueue.update(0, ("y", 4))
    limitQueue.printHeap()
    limitQueue.printKPos()
    println("w -> " + limitQueue.getPos("w"))
    println("m -> " + limitQueue.getPos("m"))
    while (!limitQueue.isEmpty) {
      val t = limitQueue.deQueue()
      println(t)
      limitQueue.printKPos()
    }
  }

  def main(args: Array[String]): Unit = {
    check(1000, 5000, 2000)
  }
}
