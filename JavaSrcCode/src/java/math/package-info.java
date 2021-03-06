/*
 * Copyright (c) 1998, 2006, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

/**
 * 提供用于执行任意精度整数算术（{@code BigInteger}）和任意精度十进制算术（{@code BigDecimal}）的类。
 * <p>
 * Provides classes for performing arbitrary-precision integer
 * arithmetic ({@code BigInteger}) and arbitrary-precision decimal
 * arithmetic ({@code BigDecimal}).  {@code BigInteger} is analogous
 * to the primitive integer types except that it provides arbitrary
 * precision, hence operations on {@code BigInteger}s do not overflow
 * or lose precision.  In addition to standard arithmetic operations,
 * {@code BigInteger} provides modular arithmetic, GCD calculation,
 * primality testing, prime generation, bit manipulation, and a few
 * other miscellaneous operations.
 * <p>
 * {@code BigDecimal} provides arbitrary-precision signed decimal
 * numbers suitable for currency calculations and the like.  {@code
 * BigDecimal} gives the user complete control over rounding behavior,
 * allowing the user to choose from a comprehensive set of eight
 * rounding modes.
 * @since JDK1.1
 */
package java.math;
