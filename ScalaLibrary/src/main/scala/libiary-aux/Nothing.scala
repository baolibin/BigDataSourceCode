/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * `Nothing`是与[[scala.Null]]一起的，位于Scala类型层次结构的底部。
  *
  * `Nothing` is - together with [[scala.Null]] - at the bottom of Scala's type hierarchy.
  *
  * `Nothing`是其他所有类型的子类型（包括[[scala.Null]]);不存在此类型的“实例”。
  * 尽管类型“Nothing”是空空如也的，然而，它在几个方面是有用的。
  * 例如，Scala库定义了一个值[[scala.collection.immutable.Nil]]类型`List[Nothing]`。
  * 因为在Scala中列表是协变的，所以[[scala.collection.immutable.Nil]]类型为“T”的任何元素的“List[T]”实例。
  *
  * `Nothing` is a subtype of every other type (including [[scala.Null]]); there exist
  * ''no instances'' of this type.  Although type `Nothing` is uninhabited, it is
  * nevertheless useful in several ways.  For instance, the Scala library defines a value
  * [[scala.collection.immutable.Nil]] of type `List[Nothing]`. Because lists are covariant in Scala,
  * this makes [[scala.collection.immutable.Nil]] an instance of `List[T]`, for any element of type `T`.
  *
  * Nothing的另一个用法是从不正常返回的方法的返回类型。
  * 一个例子是方法错误[[scala.sys]]，它总是引发异常。
  *
  * Another usage for Nothing is the return type for methods which never return normally.
  * One example is method error in [[scala.sys]], which always throws an exception.
  */
sealed trait Nothing

