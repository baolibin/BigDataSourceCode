/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


/**
  * 核心Scala类型。它们在没有显式导入的情况下始终可用。
  *
  * Core Scala types. They are always available without an explicit import.
  *
  * @contentDiagram hideNodes "scala.Serializable"
  */
package object scala {
    type Throwable = java.lang.Throwable
    type Exception = java.lang.Exception
    type Error = java.lang.Error

    type RuntimeException = java.lang.RuntimeException
    type NullPointerException = java.lang.NullPointerException
    type ClassCastException = java.lang.ClassCastException
    type IndexOutOfBoundsException = java.lang.IndexOutOfBoundsException
    type ArrayIndexOutOfBoundsException = java.lang.ArrayIndexOutOfBoundsException
    type StringIndexOutOfBoundsException = java.lang.StringIndexOutOfBoundsException
    type UnsupportedOperationException = java.lang.UnsupportedOperationException
    type IllegalArgumentException = java.lang.IllegalArgumentException
    type NoSuchElementException = java.util.NoSuchElementException
    type NumberFormatException = java.lang.NumberFormatException
    type AbstractMethodError = java.lang.AbstractMethodError
    type InterruptedException = java.lang.InterruptedException
    type TraversableOnce[+A] = scala.collection.TraversableOnce[A]
    type Traversable[+A] = scala.collection.Traversable[A]
    type Iterable[+A] = scala.collection.Iterable[A]
    type Seq[+A] = scala.collection.Seq[A]
    type IndexedSeq[+A] = scala.collection.IndexedSeq[A]
    type Iterator[+A] = scala.collection.Iterator[A]
    type BufferedIterator[+A] = scala.collection.BufferedIterator[A]
    type List[+A] = scala.collection.immutable.List[A]
    type ::[A] = scala.collection.immutable.::[A]
    type Stream[+A] = scala.collection.immutable.Stream[A]
    type Vector[+A] = scala.collection.immutable.Vector[A]
    type StringBuilder = scala.collection.mutable.StringBuilder
    type Range = scala.collection.immutable.Range
    type BigDecimal = scala.math.BigDecimal
    type BigInt = scala.math.BigInt
    type Equiv[T] = scala.math.Equiv[T]
    type Fractional[T] = scala.math.Fractional[T]
    type Integral[T] = scala.math.Integral[T]
    type Numeric[T] = scala.math.Numeric[T]
    type Ordered[T] = scala.math.Ordered[T]
    type Ordering[T] = scala.math.Ordering[T]
    type PartialOrdering[T] = scala.math.PartialOrdering[T]
    type PartiallyOrdered[T] = scala.math.PartiallyOrdered[T]
    type Either[+A, +B] = scala.util.Either[A, B]
    type Left[+A, +B] = scala.util.Left[A, B]
    type Right[+A, +B] = scala.util.Right[A, B]
    // A dummy used by the specialization annotation.
    val AnyRef = new Specializable {
        override def toString = "object AnyRef"
    }
    val Traversable = scala.collection.Traversable
    val Iterable = scala.collection.Iterable

    // Numeric types which were moved into scala.math.*
    val Seq = scala.collection.Seq
    val IndexedSeq = scala.collection.IndexedSeq
    val Iterator = scala.collection.Iterator
    val List = scala.collection.immutable.List
    val Nil = scala.collection.immutable.Nil
    val :: = scala.collection.immutable.::
    val +: = scala.collection.+:
    val :+ = scala.collection.:+
    val Stream = scala.collection.immutable.Stream
    val #:: = scala.collection.immutable.Stream.#::
    val Vector = scala.collection.immutable.Vector
    val StringBuilder = scala.collection.mutable.StringBuilder
    val Range = scala.collection.immutable.Range
    val BigDecimal = scala.math.BigDecimal
    val BigInt = scala.math.BigInt
    val Equiv = scala.math.Equiv
    val Fractional = scala.math.Fractional
    val Integral = scala.math.Integral
    val Numeric = scala.math.Numeric
    val Ordered = scala.math.Ordered
    val Ordering = scala.math.Ordering
    val Either = scala.util.Either
    val Left = scala.util.Left
    val Right = scala.util.Right

    // Annotations which we might move to annotation.*
    /*
      type SerialVersionUID = annotation.SerialVersionUID
      type deprecated = annotation.deprecated
      type deprecatedName = annotation.deprecatedName
      type inline = annotation.inline
      type native = annotation.native
      type noinline = annotation.noinline
      type remote = annotation.remote
      type specialized = annotation.specialized
      type transient = annotation.transient
      type throws  = annotation.throws
      type unchecked = annotation.unchecked.unchecked
      type volatile = annotation.volatile
      */
}
