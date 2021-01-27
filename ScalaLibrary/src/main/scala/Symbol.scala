/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package scala

/**
  * 这个类提供了一种简单的方法来获取相等字符串的唯一对象。
  * 由于符号是内部的，所以可以使用引用等式对它们进行比较。
  * “Symbol”的实例可以通过Scala内置的引号机制轻松创建。
  *
  * This class provides a simple way to get unique objects for equal strings.
  * Since symbols are interned, they can be compared using reference equality.
  * Instances of `Symbol` can be created easily with Scala's built-in quote
  * mechanism.
  *
  * For instance, the [[http://scala-lang.org/#_top Scala]] term `'mysym` will
  * invoke the constructor of the `Symbol` class in the following way:
  * `Symbol("mysym")`.
  *
  * @author Martin Odersky, Iulian Dragos
  * @version 1.8
  */
final class Symbol private(val name: String) extends Serializable {
    /** Converts this symbol to a string.
      */
    override def toString(): String = "'" + name

    override def hashCode = name.hashCode()

    override def equals(other: Any) = this eq other.asInstanceOf[AnyRef]

    @throws(classOf[java.io.ObjectStreamException])
    private def readResolve(): Any = Symbol.apply(name)
}

object Symbol extends UniquenessCache[String, Symbol] {
    override def apply(name: String): Symbol = super.apply(name)

    protected def valueFromKey(name: String): Symbol = new Symbol(name)

    protected def keyFromValue(sym: Symbol): Option[String] = Some(sym.name)
}

/** This is private so it won't appear in the library API, but
  * abstracted to offer some hope of reusability.  */
private[scala] abstract class UniquenessCache[K, V >: Null] {

    import java.lang.ref.WeakReference
    import java.util.WeakHashMap
    import java.util.concurrent.locks.ReentrantReadWriteLock

    private val rwl = new ReentrantReadWriteLock()
    private val rlock = rwl.readLock
    private val wlock = rwl.writeLock
    private val map = new WeakHashMap[K, WeakReference[V]]

    def apply(name: K): V = {
        def cached(): V = {
            rlock.lock
            try {
                val reference = map get name
                if (reference == null) null
                else reference.get // will be null if we were gc-ed
            }
            finally rlock.unlock
        }

        def updateCache(): V = {
            wlock.lock
            try {
                val res = cached()
                if (res != null) res
                else {
                    // If we don't remove the old String key from the map, we can
                    // wind up with one String as the key and a different String as
                    // as the name field in the Symbol, which can lead to surprising
                    // GC behavior and duplicate Symbols. See SI-6706.
                    map remove name
                    val sym = valueFromKey(name)
                    map.put(name, new WeakReference(sym))
                    sym
                }
            }
            finally wlock.unlock
        }

        val res = cached()
        if (res == null) updateCache()
        else res
    }

    def unapply(other: V): Option[K] = keyFromValue(other)

    protected def valueFromKey(k: K): V

    protected def keyFromValue(v: V): Option[K]
}
