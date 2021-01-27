/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * 扩展这个特性的类可以跨平台（Java，.NET）序列化。
  *
  * Classes extending this trait are serializable across platforms (Java, .NET).
  */
trait Serializable extends Any with java.io.Serializable
