/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala

/**
  * 此类表示未初始化的变量/值错误。
  *
  * This class represents uninitialized variable/value errors.
  *
  * @author Martin Odersky
  * @since 2.5
  */
final class UninitializedError extends RuntimeException("uninitialized value")
