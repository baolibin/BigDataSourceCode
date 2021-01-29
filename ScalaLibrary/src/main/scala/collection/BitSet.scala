/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */


package collection

/**
  * 可变和不可变位集的公共基类。
  *
  * A common base class for mutable and immutable bitsets.
  * $bitsetinfo
  */
trait BitSet extends SortedSet[Int]
        with BitSetLike[BitSet] {
    override def empty: BitSet = BitSet.empty
}

/** $factoryInfo
  *
  * @define coll bitset
  * @define Coll `BitSet`
  */
object BitSet extends BitSetFactory[BitSet] {
    val empty: BitSet = immutable.BitSet.empty

    def newBuilder = immutable.BitSet.newBuilder

    /** $canBuildFromInfo */
    implicit def canBuildFrom: CanBuildFrom[BitSet, Int, BitSet] = bitsetCanBuildFrom
}

