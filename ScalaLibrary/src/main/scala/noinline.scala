/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package scala

/**
  * 方法上的注释，禁止编译器内联该方法，无论内联看起来多么安全。
  *
  * An annotation on methods that forbids the compiler to inline the
  * method, no matter how safe the inlining appears to be.
  *
  * @author Lex Spoon
  * @version 1.0, 2007-5-21
  * @since 2.5
  */
class noinline extends scala.annotation.StaticAnnotation
