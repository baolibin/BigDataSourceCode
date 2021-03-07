/*
 * Copyright (c) 1997, 2013, Oracle and/or its affiliates. All rights reserved.
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

package java.util;

import java.util.function.Consumer;

/**
 * {@code list}和{@code Deque}接口的双链表实现。
 * 实现所有可选的列表操作，并允许所有元素（包括{@code null}）。
 * <p>
 * Doubly-linked list implementation of the {@code List} and {@code Deque}
 * interfaces.  Implements all optional list operations, and permits all
 * elements (including {@code null}).
 *
 * <p>All of the operations perform as could be expected for a doubly-linked
 * list.  Operations that index into the list will traverse the list from
 * the beginning or the end, whichever is closer to the specified index.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 * If multiple threads access a linked list concurrently, and at least
 * one of the threads modifies the list structurally, it <i>must</i> be
 * synchronized externally.  (A structural modification is any operation
 * that adds or deletes one or more elements; merely setting the value of
 * an element is not a structural modification.)  This is typically
 * accomplished by synchronizing on some object that naturally
 * encapsulates the list.
 * <p>
 * If no such object exists, the list should be "wrapped" using the
 * {@link Collections#synchronizedList Collections.synchronizedList}
 * method.  This is best done at creation time, to prevent accidental
 * unsynchronized access to the list:<pre>
 *   List list = Collections.synchronizedList(new LinkedList(...));</pre>
 *
 * <p>The iterators returned by this class's {@code iterator} and
 * {@code listIterator} methods are <i>fail-fast</i>: if the list is
 * structurally modified at any time after the iterator is created, in
 * any way except through the Iterator's own {@code remove} or
 * {@code add} methods, the iterator will throw a {@link
 * ConcurrentModificationException}.  Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than
 * risking arbitrary, non-deterministic behavior at an undetermined
 * time in the future.
 *
 * <p>Note that the fail-fast behavior of an iterator cannot be guaranteed
 * as it is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification.  Fail-fast iterators
 * throw {@code ConcurrentModificationException} on a best-effort basis.
 * Therefore, it would be wrong to write a program that depended on this
 * exception for its correctness:   <i>the fail-fast behavior of iterators
 * should be used only to detect bugs.</i>
 *
 * <p>This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html">
 * Java Collections Framework</a>.
 * @param <E> the type of elements held in this collection
 * @author Josh Bloch
 * @see List
 * @see ArrayList
 * @since 1.2
 */

public class LinkedList<E>
		extends AbstractSequentialList<E>
		implements List<E>, Deque<E>, Cloneable, java.io.Serializable {
	transient int size = 0;

	/**
	 * 指向上一个节点的指针。
	 * <p>
	 * Pointer to first node.
	 * Invariant: (first == null && last == null) ||
	 * (first.prev == null && first.item != null)
	 */
	transient Node<E> first;

	/**
	 * 指向下一个节点的指针。
	 * <p>
	 * Pointer to last node.
	 * Invariant: (first == null && last == null) ||
	 * (last.next == null && last.item != null)
	 */
	transient Node<E> last;

	/**
     * 构造一个空列表。
     *
	 * Constructs an empty list.
	 */
	public LinkedList() {
	}

	/**
     * 按照集合迭代器返回元素的顺序，构造一个包含指定集合元素的列表。
     *
	 * Constructs a list containing the elements of the specified
	 * collection, in the order they are returned by the collection's
	 * iterator.
	 * @param c the collection whose elements are to be placed into this list
	 * @throws NullPointerException if the specified collection is null
	 */
	public LinkedList(Collection<? extends E> c) {
		this();
		addAll(c);
	}

	/**
     * 链接e作为第一个元素。
     *
	 * Links e as first element.
	 */
	private void linkFirst(E e) {
		final Node<E> f = first;
		final Node<E> newNode = new Node<>(null, e, f);
		first = newNode;
		if (f == null)
			last = newNode;
		else
			f.prev = newNode;
		size++;
		modCount++;
	}

	/**
     * 链接e作为最后一个元素。
     *
	 * Links e as last element.
	 */
	void linkLast(E e) {
		final Node<E> l = last;
		final Node<E> newNode = new Node<>(l, e, null);
		last = newNode;
		if (l == null)
			first = newNode;
		else
			l.next = newNode;
		size++;
		modCount++;
	}

	/**
     * 在非空节点成功之前插入元素e。
     *
	 * Inserts element e before non-null Node succ.
	 */
	void linkBefore(E e, Node<E> succ) {
		// assert succ != null;
		final Node<E> pred = succ.prev;
		final Node<E> newNode = new Node<>(pred, e, succ);
		succ.prev = newNode;
		if (pred == null)
			first = newNode;
		else
			pred.next = newNode;
		size++;
		modCount++;
	}

	/**
     * 取消非空第一个节点f的链接。
     *
	 * Unlinks non-null first node f.
	 */
	private E unlinkFirst(Node<E> f) {
		// assert f == first && f != null;
		final E element = f.item;
		final Node<E> next = f.next;
		f.item = null;
		f.next = null; // help GC
		first = next;
		if (next == null)
			last = null;
		else
			next.prev = null;
		size--;
		modCount++;
		return element;
	}

	/**
     * 取消非空最后一个节点l的链接
     *
	 * Unlinks non-null last node l.
	 */
	private E unlinkLast(Node<E> l) {
		// assert l == last && l != null;
		final E element = l.item;
		final Node<E> prev = l.prev;
		l.item = null;
		l.prev = null; // help GC
		last = prev;
		if (prev == null)
			first = null;
		else
			prev.next = null;
		size--;
		modCount++;
		return element;
	}

	/**
     * 取消非空节点x的链接。
     *
	 * Unlinks non-null node x.
	 */
	E unlink(Node<E> x) {
		// assert x != null;
		final E element = x.item;
		final Node<E> next = x.next;
		final Node<E> prev = x.prev;

		if (prev == null) {
			first = next;
		} else {
			prev.next = next;
			x.prev = null;
		}

		if (next == null) {
			last = prev;
		} else {
			next.prev = prev;
			x.next = null;
		}

		x.item = null;
		size--;
		modCount++;
		return element;
	}

	/**
     * 返回此列表中的第一个元素。
     *
	 * Returns the first element in this list.
	 * @return the first element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public E getFirst() {
		final Node<E> f = first;
		if (f == null)
			throw new NoSuchElementException();
		return f.item;
	}

	/**
     * 返回此列表中的最后一个元素。
     *
	 * Returns the last element in this list.
	 * @return the last element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public E getLast() {
		final Node<E> l = last;
		if (l == null)
			throw new NoSuchElementException();
		return l.item;
	}

	/**
     * 从列表中删除并返回第一个元素。
     *
	 * Removes and returns the first element from this list.
	 * @return the first element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public E removeFirst() {
		final Node<E> f = first;
		if (f == null)
			throw new NoSuchElementException();
		return unlinkFirst(f);
	}

	/**
     * 删除并返回此列表中的最后一个元素。
     *
	 * Removes and returns the last element from this list.
	 * @return the last element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public E removeLast() {
		final Node<E> l = last;
		if (l == null)
			throw new NoSuchElementException();
		return unlinkLast(l);
	}

	/**
     * 在此列表的开头插入指定的元素。
     *
	 * Inserts the specified element at the beginning of this list.
	 * @param e the element to add
	 */
	public void addFirst(E e) {
		linkFirst(e);
	}

	/**
     * 将指定的元素追加到此列表的末尾。
     *
	 * Appends the specified element to the end of this list.
	 *
	 * <p>This method is equivalent to {@link #add}.
	 * @param e the element to add
	 */
	public void addLast(E e) {
		linkLast(e);
	}

	/**
     * 如果此列表包含指定的元素，则返回{@code true}。
     *
	 * Returns {@code true} if this list contains the specified element.
	 * More formally, returns {@code true} if and only if this list contains
	 * at least one element {@code e} such that
	 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
	 * @param o element whose presence in this list is to be tested
	 * @return {@code true} if this list contains the specified element
	 */
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}

	/**
     * 返回此列表中的元素数。
     *
	 * Returns the number of elements in this list.
	 * @return the number of elements in this list
	 */
	public int size() {
		return size;
	}

	/**
     * 将指定的元素追加到此列表的末尾。
     *
	 * Appends the specified element to the end of this list.
	 *
	 * <p>This method is equivalent to {@link #addLast}.
	 * @param e element to be appended to this list
	 * @return {@code true} (as specified by {@link Collection#add})
	 */
	public boolean add(E e) {
		linkLast(e);
		return true;
	}

	/**
     * 从此列表中删除指定元素的第一个匹配项（如果存在）。
     *
	 * Removes the first occurrence of the specified element from this list,
	 * if it is present.  If this list does not contain the element, it is
	 * unchanged.  More formally, removes the element with the lowest index
	 * {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
	 * (if such an element exists).  Returns {@code true} if this list
	 * contained the specified element (or equivalently, if this list
	 * changed as a result of the call).
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if this list contained the specified element
	 */
	public boolean remove(Object o) {
		if (o == null) {
			for (Node<E> x = first; x != null; x = x.next) {
				if (x.item == null) {
					unlink(x);
					return true;
				}
			}
		} else {
			for (Node<E> x = first; x != null; x = x.next) {
				if (o.equals(x.item)) {
					unlink(x);
					return true;
				}
			}
		}
		return false;
	}

	/**
     * 将指定集合中的所有元素按指定集合的迭代器返回的顺序追加到此列表的末尾。
     *
	 * Appends all of the elements in the specified collection to the end of
	 * this list, in the order that they are returned by the specified
	 * collection's iterator.  The behavior of this operation is undefined if
	 * the specified collection is modified while the operation is in
	 * progress.  (Note that this will occur if the specified collection is
	 * this list, and it's nonempty.)
	 * @param c collection containing elements to be added to this list
	 * @return {@code true} if this list changed as a result of the call
	 * @throws NullPointerException if the specified collection is null
	 */
	public boolean addAll(Collection<? extends E> c) {
		return addAll(size, c);
	}

	/**
     * 从指定位置开始，将指定集合中的所有元素插入此列表。
     *
	 * Inserts all of the elements in the specified collection into this
	 * list, starting at the specified position.  Shifts the element
	 * currently at that position (if any) and any subsequent elements to
	 * the right (increases their indices).  The new elements will appear
	 * in the list in the order that they are returned by the
	 * specified collection's iterator.
	 * @param index index at which to insert the first element
	 *              from the specified collection
	 * @param c     collection containing elements to be added to this list
	 * @return {@code true} if this list changed as a result of the call
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 * @throws NullPointerException      if the specified collection is null
	 */
	public boolean addAll(int index, Collection<? extends E> c) {
		checkPositionIndex(index);

		Object[] a = c.toArray();
		int numNew = a.length;
		if (numNew == 0)
			return false;

		Node<E> pred, succ;
		if (index == size) {
			succ = null;
			pred = last;
		} else {
			succ = node(index);
			pred = succ.prev;
		}

		for (Object o : a) {
			@SuppressWarnings("unchecked") E e = (E) o;
			Node<E> newNode = new Node<>(pred, e, null);
			if (pred == null)
				first = newNode;
			else
				pred.next = newNode;
			pred = newNode;
		}

		if (succ == null) {
			last = pred;
		} else {
			pred.next = succ;
			succ.prev = pred;
		}

		size += numNew;
		modCount++;
		return true;
	}

	/**
     * 从此列表中删除所有元素。
     *
	 * Removes all of the elements from this list.
	 * The list will be empty after this call returns.
	 */
	public void clear() {
		// Clearing all of the links between nodes is "unnecessary", but:
		// - helps a generational GC if the discarded nodes inhabit
		//   more than one generation
		// - is sure to free memory even if there is a reachable Iterator
		for (Node<E> x = first; x != null; ) {
			Node<E> next = x.next;
			x.item = null;
			x.next = null;
			x.prev = null;
			x = next;
		}
		first = last = null;
		size = 0;
		modCount++;
	}


	// Positional Access Operations

	/**
     * 返回此列表中指定位置的元素。
     *
	 * Returns the element at the specified position in this list.
	 * @param index index of the element to return
	 * @return the element at the specified position in this list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E get(int index) {
		checkElementIndex(index);
		return node(index).item;
	}

	/**
     * 用指定的元素替换此列表中指定位置的元素。
     *
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 * @param index   index of the element to replace
	 * @param element element to be stored at the specified position
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E set(int index, E element) {
		checkElementIndex(index);
		Node<E> x = node(index);
		E oldVal = x.item;
		x.item = element;
		return oldVal;
	}

	/**
     * 在此列表中的指定位置插入指定的元素。
     *
	 * Inserts the specified element at the specified position in this list.
	 * Shifts the element currently at that position (if any) and any
	 * subsequent elements to the right (adds one to their indices).
	 * @param index   index at which the specified element is to be inserted
	 * @param element element to be inserted
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public void add(int index, E element) {
		checkPositionIndex(index);

		if (index == size)
			linkLast(element);
		else
			linkBefore(element, node(index));
	}

	/**
     * 删除此列表中指定位置的元素。
     *
	 * Removes the element at the specified position in this list.  Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 * Returns the element that was removed from the list.
	 * @param index the index of the element to be removed
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public E remove(int index) {
		checkElementIndex(index);
		return unlink(node(index));
	}

	/**
     * 说明参数是否是现有元素的索引。
     *
	 * Tells if the argument is the index of an existing element.
	 */
	private boolean isElementIndex(int index) {
		return index >= 0 && index < size;
	}

	/**
     * 说明参数是迭代器或add操作的有效位置的索引。
     *
	 * Tells if the argument is the index of a valid position for an
	 * iterator or an add operation.
	 */
	private boolean isPositionIndex(int index) {
		return index >= 0 && index <= size;
	}

	/**
     * 构造IndexOutOfBoundsException详细信息消息。
     *
	 * Constructs an IndexOutOfBoundsException detail message.
	 * Of the many possible refactorings of the error handling code,
	 * this "outlining" performs best with both server and client VMs.
	 */
	private String outOfBoundsMsg(int index) {
		return "Index: " + index + ", Size: " + size;
	}

	private void checkElementIndex(int index) {
		if (!isElementIndex(index))
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	private void checkPositionIndex(int index) {
		if (!isPositionIndex(index))
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	/**
     * 返回指定元素索引处的（非空）节点。
     *
	 * Returns the (non-null) Node at the specified element index.
	 */
	Node<E> node(int index) {
		// assert isElementIndex(index);

		if (index < (size >> 1)) {
			Node<E> x = first;
			for (int i = 0; i < index; i++)
				x = x.next;
			return x;
		} else {
			Node<E> x = last;
			for (int i = size - 1; i > index; i--)
				x = x.prev;
			return x;
		}
	}

	// Search Operations

	/**
     * 返回此列表中指定元素第一次出现的索引，如果此列表不包含该元素，则返回-1。
     *
	 * Returns the index of the first occurrence of the specified element
	 * in this list, or -1 if this list does not contain the element.
	 * More formally, returns the lowest index {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 * @param o element to search for
	 * @return the index of the first occurrence of the specified element in
	 * this list, or -1 if this list does not contain the element
	 */
	public int indexOf(Object o) {
		int index = 0;
		if (o == null) {
			for (Node<E> x = first; x != null; x = x.next) {
				if (x.item == null)
					return index;
				index++;
			}
		} else {
			for (Node<E> x = first; x != null; x = x.next) {
				if (o.equals(x.item))
					return index;
				index++;
			}
		}
		return -1;
	}

	/**
     * 返回此列表中指定元素最后一次出现的索引，如果此列表不包含该元素，则返回-1。
     *
	 * Returns the index of the last occurrence of the specified element
	 * in this list, or -1 if this list does not contain the element.
	 * More formally, returns the highest index {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 * @param o element to search for
	 * @return the index of the last occurrence of the specified element in
	 * this list, or -1 if this list does not contain the element
	 */
	public int lastIndexOf(Object o) {
		int index = size;
		if (o == null) {
			for (Node<E> x = last; x != null; x = x.prev) {
				index--;
				if (x.item == null)
					return index;
			}
		} else {
			for (Node<E> x = last; x != null; x = x.prev) {
				index--;
				if (o.equals(x.item))
					return index;
			}
		}
		return -1;
	}

	// Queue operations.

	/**
     * 检索但不删除此列表的头（第一个元素）。
     *
	 * Retrieves, but does not remove, the head (first element) of this list.
	 * @return the head of this list, or {@code null} if this list is empty
	 * @since 1.5
	 */
	public E peek() {
		final Node<E> f = first;
		return (f == null) ? null : f.item;
	}

	/**
     * 检索但不删除此列表的头（第一个元素）。
     *
	 * Retrieves, but does not remove, the head (first element) of this list.
	 * @return the head of this list
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.5
	 */
	public E element() {
		return getFirst();
	}

	/**
     * 检索并删除此列表的头（第一个元素）。
     *
	 * Retrieves and removes the head (first element) of this list.
	 * @return the head of this list, or {@code null} if this list is empty
	 * @since 1.5
	 */
	public E poll() {
		final Node<E> f = first;
		return (f == null) ? null : unlinkFirst(f);
	}

	/**
     * 检索并删除此列表的头（第一个元素）。
     *
	 * Retrieves and removes the head (first element) of this list.
	 * @return the head of this list
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.5
	 */
	public E remove() {
		return removeFirst();
	}

	/**
     * 添加指定的元素作为此列表的尾部（最后一个元素）。
     *
	 * Adds the specified element as the tail (last element) of this list.
	 * @param e the element to add
	 * @return {@code true} (as specified by {@link Queue#offer})
	 * @since 1.5
	 */
	public boolean offer(E e) {
		return add(e);
	}

	// Deque operations

	/**
     * 在此列表前面插入指定的元素。
     *
	 * Inserts the specified element at the front of this list.
	 * @param e the element to insert
	 * @return {@code true} (as specified by {@link Deque#offerFirst})
	 * @since 1.6
	 */
	public boolean offerFirst(E e) {
		addFirst(e);
		return true;
	}

	/**
     * 在此列表末尾插入指定的元素。
     *
	 * Inserts the specified element at the end of this list.
	 * @param e the element to insert
	 * @return {@code true} (as specified by {@link Deque#offerLast})
	 * @since 1.6
	 */
	public boolean offerLast(E e) {
		addLast(e);
		return true;
	}

	/**
     * 检索但不删除此列表的第一个元素，如果此列表为空，则返回{@code null}。
     *
	 * Retrieves, but does not remove, the first element of this list,
	 * or returns {@code null} if this list is empty.
	 * @return the first element of this list, or {@code null}
	 * if this list is empty
	 * @since 1.6
	 */
	public E peekFirst() {
		final Node<E> f = first;
		return (f == null) ? null : f.item;
	}

	/**
     * 检索但不删除此列表的最后一个元素，如果此列表为空，则返回{@code null}。
     *
	 * Retrieves, but does not remove, the last element of this list,
	 * or returns {@code null} if this list is empty.
	 * @return the last element of this list, or {@code null}
	 * if this list is empty
	 * @since 1.6
	 */
	public E peekLast() {
		final Node<E> l = last;
		return (l == null) ? null : l.item;
	}

	/**
     * 检索并删除此列表的第一个元素，如果此列表为空，则返回{@code null}。
     *
	 * Retrieves and removes the first element of this list,
	 * or returns {@code null} if this list is empty.
	 * @return the first element of this list, or {@code null} if
	 * this list is empty
	 * @since 1.6
	 */
	public E pollFirst() {
		final Node<E> f = first;
		return (f == null) ? null : unlinkFirst(f);
	}

	/**
     * 检索并删除此列表的最后一个元素，如果此列表为空，则返回{@code null}。
     *
	 * Retrieves and removes the last element of this list,
	 * or returns {@code null} if this list is empty.
	 * @return the last element of this list, or {@code null} if
	 * this list is empty
	 * @since 1.6
	 */
	public E pollLast() {
		final Node<E> l = last;
		return (l == null) ? null : unlinkLast(l);
	}

	/**
     * 将元素推送到由该列表表示的堆栈上。换句话说，在列表的前面插入元素。
     *
	 * Pushes an element onto the stack represented by this list.  In other
	 * words, inserts the element at the front of this list.
	 *
	 * <p>This method is equivalent to {@link #addFirst}.
	 * @param e the element to push
	 * @since 1.6
	 */
	public void push(E e) {
		addFirst(e);
	}

	/**
     * 从该列表表示的堆栈中弹出一个元素。换句话说，删除并返回此列表的第一个元素。
     *
	 * Pops an element from the stack represented by this list.  In other
	 * words, removes and returns the first element of this list.
	 *
	 * <p>This method is equivalent to {@link #removeFirst()}.
	 * @return the element at the front of this list (which is the top
	 * of the stack represented by this list)
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.6
	 */
	public E pop() {
		return removeFirst();
	}

	/**
     * 删除此列表中指定元素的第一个匹配项（从头到尾遍历列表时）。
     * 如果列表不包含该元素，则该元素保持不变。
     *
	 * Removes the first occurrence of the specified element in this
	 * list (when traversing the list from head to tail).  If the list
	 * does not contain the element, it is unchanged.
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if the list contained the specified element
	 * @since 1.6
	 */
	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	/**
     * 删除此列表中指定元素的最后一次出现（从头到尾遍历列表时）。
     * 如果列表不包含该元素，则该元素保持不变。
     *
	 * Removes the last occurrence of the specified element in this
	 * list (when traversing the list from head to tail).  If the list
	 * does not contain the element, it is unchanged.
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if the list contained the specified element
	 * @since 1.6
	 */
	public boolean removeLastOccurrence(Object o) {
		if (o == null) {
			for (Node<E> x = last; x != null; x = x.prev) {
				if (x.item == null) {
					unlink(x);
					return true;
				}
			}
		} else {
			for (Node<E> x = last; x != null; x = x.prev) {
				if (o.equals(x.item)) {
					unlink(x);
					return true;
				}
			}
		}
		return false;
	}

	/**
     * 返回此列表中元素的列表迭代器（按正确顺序），从列表中的指定位置开始。
     *
	 * Returns a list-iterator of the elements in this list (in proper
	 * sequence), starting at the specified position in the list.
	 * Obeys the general contract of {@code List.listIterator(int)}.<p>
	 * <p>
	 * The list-iterator is <i>fail-fast</i>: if the list is structurally
	 * modified at any time after the Iterator is created, in any way except
	 * through the list-iterator's own {@code remove} or {@code add}
	 * methods, the list-iterator will throw a
	 * {@code ConcurrentModificationException}.  Thus, in the face of
	 * concurrent modification, the iterator fails quickly and cleanly, rather
	 * than risking arbitrary, non-deterministic behavior at an undetermined
	 * time in the future.
	 * @param index index of the first element to be returned from the
	 *              list-iterator (by a call to {@code next})
	 * @return a ListIterator of the elements in this list (in proper
	 * sequence), starting at the specified position in the list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 * @see List#listIterator(int)
	 */
	public ListIterator<E> listIterator(int index) {
		checkPositionIndex(index);
		return new ListItr(index);
	}

	private class ListItr implements ListIterator<E> {
		private Node<E> lastReturned;
		private Node<E> next;
		private int nextIndex;
		private int expectedModCount = modCount;

		ListItr(int index) {
			// assert isPositionIndex(index);
			next = (index == size) ? null : node(index);
			nextIndex = index;
		}

		public boolean hasNext() {
			return nextIndex < size;
		}

		public E next() {
			checkForComodification();
			if (!hasNext())
				throw new NoSuchElementException();

			lastReturned = next;
			next = next.next;
			nextIndex++;
			return lastReturned.item;
		}

		public boolean hasPrevious() {
			return nextIndex > 0;
		}

		public E previous() {
			checkForComodification();
			if (!hasPrevious())
				throw new NoSuchElementException();

			lastReturned = next = (next == null) ? last : next.prev;
			nextIndex--;
			return lastReturned.item;
		}

		public int nextIndex() {
			return nextIndex;
		}

		public int previousIndex() {
			return nextIndex - 1;
		}

		public void remove() {
			checkForComodification();
			if (lastReturned == null)
				throw new IllegalStateException();

			Node<E> lastNext = lastReturned.next;
			unlink(lastReturned);
			if (next == lastReturned)
				next = lastNext;
			else
				nextIndex--;
			lastReturned = null;
			expectedModCount++;
		}

		public void set(E e) {
			if (lastReturned == null)
				throw new IllegalStateException();
			checkForComodification();
			lastReturned.item = e;
		}

		public void add(E e) {
			checkForComodification();
			lastReturned = null;
			if (next == null)
				linkLast(e);
			else
				linkBefore(e, next);
			nextIndex++;
			expectedModCount++;
		}

		public void forEachRemaining(Consumer<? super E> action) {
			Objects.requireNonNull(action);
			while (modCount == expectedModCount && nextIndex < size) {
				action.accept(next.item);
				lastReturned = next;
				next = next.next;
				nextIndex++;
			}
			checkForComodification();
		}

		final void checkForComodification() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}
	}

	private static class Node<E> {
		E item;
		Node<E> next;
		Node<E> prev;

		Node(Node<E> prev, E element, Node<E> next) {
			this.item = element;
			this.next = next;
			this.prev = prev;
		}
	}

	/**
	 * @since 1.6
	 */
	public Iterator<E> descendingIterator() {
		return new DescendingIterator();
	}

	/**
	 * Adapter to provide descending iterators via ListItr.previous
	 */
	private class DescendingIterator implements Iterator<E> {
		private final ListItr itr = new ListItr(size());

		public boolean hasNext() {
			return itr.hasPrevious();
		}

		public E next() {
			return itr.previous();
		}

		public void remove() {
			itr.remove();
		}
	}

	@SuppressWarnings("unchecked")
	private LinkedList<E> superClone() {
		try {
			return (LinkedList<E>) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}

	/**
     * 返回此{@code LinkedList}的浅表副本。（元素本身没有克隆。）
     *
	 * Returns a shallow copy of this {@code LinkedList}. (The elements
	 * themselves are not cloned.)
	 * @return a shallow copy of this {@code LinkedList} instance
	 */
	public Object clone() {
		LinkedList<E> clone = superClone();

		// Put clone into "virgin" state
		clone.first = clone.last = null;
		clone.size = 0;
		clone.modCount = 0;

		// Initialize clone with our elements
		for (Node<E> x = first; x != null; x = x.next)
			clone.add(x.item);

		return clone;
	}

	/**
     * 返回一个数组，该数组按正确的顺序（从第一个元素到最后一个元素）包含此列表中的所有元素。
     *
	 * Returns an array containing all of the elements in this list
	 * in proper sequence (from first to last element).
	 *
	 * <p>The returned array will be "safe" in that no references to it are
	 * maintained by this list.  (In other words, this method must allocate
	 * a new array).  The caller is thus free to modify the returned array.
	 *
	 * <p>This method acts as bridge between array-based and collection-based
	 * APIs.
	 * @return an array containing all of the elements in this list
	 * in proper sequence
	 */
	public Object[] toArray() {
		Object[] result = new Object[size];
		int i = 0;
		for (Node<E> x = first; x != null; x = x.next)
			result[i++] = x.item;
		return result;
	}

	/**
     * 返回一个数组，该数组按正确的顺序（从第一个元素到最后一个元素）包含此列表中的所有元素；
     * 返回数组的运行时类型是指定数组的运行时类型。
     *
	 * Returns an array containing all of the elements in this list in
	 * proper sequence (from first to last element); the runtime type of
	 * the returned array is that of the specified array.  If the list fits
	 * in the specified array, it is returned therein.  Otherwise, a new
	 * array is allocated with the runtime type of the specified array and
	 * the size of this list.
	 *
	 * <p>If the list fits in the specified array with room to spare (i.e.,
	 * the array has more elements than the list), the element in the array
	 * immediately following the end of the list is set to {@code null}.
	 * (This is useful in determining the length of the list <i>only</i> if
	 * the caller knows that the list does not contain any null elements.)
	 *
	 * <p>Like the {@link #toArray()} method, this method acts as bridge between
	 * array-based and collection-based APIs.  Further, this method allows
	 * precise control over the runtime type of the output array, and may,
	 * under certain circumstances, be used to save allocation costs.
	 *
	 * <p>Suppose {@code x} is a list known to contain only strings.
	 * The following code can be used to dump the list into a newly
	 * allocated array of {@code String}:
	 *
	 * <pre>
	 *     String[] y = x.toArray(new String[0]);</pre>
	 * <p>
	 * Note that {@code toArray(new Object[0])} is identical in function to
	 * {@code toArray()}.
	 * @param a the array into which the elements of the list are to
	 *          be stored, if it is big enough; otherwise, a new array of the
	 *          same runtime type is allocated for this purpose.
	 * @return an array containing the elements of the list
	 * @throws ArrayStoreException  if the runtime type of the specified array
	 *                              is not a supertype of the runtime type of every element in
	 *                              this list
	 * @throws NullPointerException if the specified array is null
	 */
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		if (a.length < size)
			a = (T[]) java.lang.reflect.Array.newInstance(
					a.getClass().getComponentType(), size);
		int i = 0;
		Object[] result = a;
		for (Node<E> x = first; x != null; x = x.next)
			result[i++] = x.item;

		if (a.length > size)
			a[size] = null;

		return a;
	}

	private static final long serialVersionUID = 876323262645176354L;

	/**
     * 将此{@code LinkedList}实例的状态保存到流（即序列化它）。
     *
	 * Saves the state of this {@code LinkedList} instance to a stream
	 * (that is, serializes it).
	 * @serialData The size of the list (the number of elements it
	 * contains) is emitted (int), followed by all of its
	 * elements (each an Object) in the proper order.
	 */
	private void writeObject(java.io.ObjectOutputStream s)
			throws java.io.IOException {
		// Write out any hidden serialization magic
		s.defaultWriteObject();

		// Write out size
		s.writeInt(size);

		// Write out all elements in the proper order.
		for (Node<E> x = first; x != null; x = x.next)
			s.writeObject(x.item);
	}

	/**
     * 从流中重新构造这个{@code LinkedList}实例（也就是说，反序列化它）。
     *
	 * Reconstitutes this {@code LinkedList} instance from a stream
	 * (that is, deserializes it).
	 */
	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream s)
			throws java.io.IOException, ClassNotFoundException {
		// Read in any hidden serialization magic
		s.defaultReadObject();

		// Read in size
		int size = s.readInt();

		// Read in all elements in the proper order.
		for (int i = 0; i < size; i++)
			linkLast((E) s.readObject());
	}

	/**
	 * Creates a <em><a href="Spliterator.html#binding">late-binding</a></em>
	 * and <em>fail-fast</em> {@link Spliterator} over the elements in this
	 * list.
	 *
	 * <p>The {@code Spliterator} reports {@link Spliterator#SIZED} and
	 * {@link Spliterator#ORDERED}.  Overriding implementations should document
	 * the reporting of additional characteristic values.
	 * @return a {@code Spliterator} over the elements in this list
	 * @implNote The {@code Spliterator} additionally reports {@link Spliterator#SUBSIZED}
	 * and implements {@code trySplit} to permit limited parallelism..
	 * @since 1.8
	 */
	@Override
	public Spliterator<E> spliterator() {
		return new LLSpliterator<E>(this, -1, 0);
	}

	/**
	 * A customized variant of Spliterators.IteratorSpliterator
	 */
	static final class LLSpliterator<E> implements Spliterator<E> {
		static final int BATCH_UNIT = 1 << 10;  // batch array size increment
		static final int MAX_BATCH = 1 << 25;  // max batch array size;
		final LinkedList<E> list; // null OK unless traversed
		Node<E> current;      // current node; null until initialized
		int est;              // size estimate; -1 until first needed
		int expectedModCount; // initialized when est set
		int batch;            // batch size for splits

		LLSpliterator(LinkedList<E> list, int est, int expectedModCount) {
			this.list = list;
			this.est = est;
			this.expectedModCount = expectedModCount;
		}

		final int getEst() {
			int s; // force initialization
			final LinkedList<E> lst;
			if ((s = est) < 0) {
				if ((lst = list) == null)
					s = est = 0;
				else {
					expectedModCount = lst.modCount;
					current = lst.first;
					s = est = lst.size;
				}
			}
			return s;
		}

		public long estimateSize() {
			return (long) getEst();
		}

		public Spliterator<E> trySplit() {
			Node<E> p;
			int s = getEst();
			if (s > 1 && (p = current) != null) {
				int n = batch + BATCH_UNIT;
				if (n > s)
					n = s;
				if (n > MAX_BATCH)
					n = MAX_BATCH;
				Object[] a = new Object[n];
				int j = 0;
				do {
					a[j++] = p.item;
				} while ((p = p.next) != null && j < n);
				current = p;
				batch = j;
				est = s - j;
				return Spliterators.spliterator(a, 0, j, Spliterator.ORDERED);
			}
			return null;
		}

		public void forEachRemaining(Consumer<? super E> action) {
			Node<E> p;
			int n;
			if (action == null) throw new NullPointerException();
			if ((n = getEst()) > 0 && (p = current) != null) {
				current = null;
				est = 0;
				do {
					E e = p.item;
					p = p.next;
					action.accept(e);
				} while (p != null && --n > 0);
			}
			if (list.modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}

		public boolean tryAdvance(Consumer<? super E> action) {
			Node<E> p;
			if (action == null) throw new NullPointerException();
			if (getEst() > 0 && (p = current) != null) {
				--est;
				E e = p.item;
				current = p.next;
				action.accept(e);
				if (list.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				return true;
			}
			return false;
		}

		public int characteristics() {
			return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
		}
	}

}
