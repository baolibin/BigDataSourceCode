/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * 不允许为空的事物的标记特征。
  *
  * A marker trait for things that are not allowed to be null
  *
  * @since 2.5
  */

@deprecated("This trait will be removed", "2.11.0")
trait NotNull extends Any {}
