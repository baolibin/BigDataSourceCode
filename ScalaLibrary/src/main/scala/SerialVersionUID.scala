/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
*/

package scala

/**
  * 用于指定可序列化类的“static SerialVersionUID”字段的批注。
  *
  * Annotation for specifying the `static SerialVersionUID` field
  * of a serializable class.
  */
class SerialVersionUID(value: Long) extends scala.annotation.ClassfileAnnotation
