/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

package collection.concurrent;

import scala.collection.concurrent.BasicNode;
import scala.collection.concurrent.Gen;
import scala.collection.concurrent.MainNode;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

abstract class INodeBase<K, V> extends scala.collection.concurrent.BasicNode {

    @SuppressWarnings("rawtypes")
    public static final AtomicReferenceFieldUpdater<INodeBase, scala.collection.concurrent.MainNode> updater =
            AtomicReferenceFieldUpdater.newUpdater(INodeBase.class, scala.collection.concurrent.MainNode.class, "mainnode");

    public static final Object RESTART = new Object();

    public volatile scala.collection.concurrent.MainNode<K, V> mainnode = null;

    public final scala.collection.concurrent.Gen gen;

    public INodeBase(scala.collection.concurrent.Gen generation) {
	gen = generation;
    }

    public BasicNode prev() {
	return null;
    }

}