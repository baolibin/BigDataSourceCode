/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * 基本类型伴随类的一种常见的超类型。
  *
  * A common supertype for companion classes of primitive types.
  *
  * A common trait for /companion/ objects of primitive types comes handy
  * when parameterizing code on types. For instance, the specialized
  * annotation is passed a sequence of types on which to specialize:
  * {{{
  *     class Tuple1[@specialized(Unit, Int, Double) T]
  * }}}
  *
  */
private[scala] trait AnyValCompanion extends Specializable {}
