/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * `Null`是与[[scala.Nothing]]一起的，位于Scala类型层次结构的底部。
  *
  * `Null` is - together with [[scala.Nothing]] - at the bottom of the Scala type hierarchy.
  *
  * `Null`是所有引用类型的子类型；它的唯一实例是'Null'引用。
  * 由于“Null”不是值类型的子类型，“Null”不是任何此类类型的成员。
  * 例如，不能将“null”赋给类型为的变量[[scala.Int]].
  *
  * `Null` is a subtype of all reference types; its only instance is the `null` reference.
  * Since `Null` is not a subtype of value types, `null` is not a member of any such type.  For instance,
  * it is not possible to assign `null` to a variable of type [[scala.Int]].
  */
sealed trait Null
