/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

import scala.annotation.meta._

/**
  * 一种注释，指明某个定义已被否决。对成员的访问将生成一个不推荐使用的警告。
  *
  * An annotation that designates that a definition is deprecated.
  * Access to the member then generates a deprecated warning.
  *
  * @param  message the message to print during compilation if the definition is accessed
  * @param  since   a string identifying the first version in which the definition was deprecated
  * @since 2.3
  */
@getter
@setter
@beanGetter
@beanSetter
class deprecated(message: String = "", since: String = "") extends scala.annotation.StaticAnnotation
