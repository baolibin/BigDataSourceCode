#### JavaSrc源码阅读
    JavaSrc模块源码阅读，版本1.8.0。

-----
##### 1、applet模块源码

-----
##### 2、awt模块源码

-----
##### 3、beans模块源码

-----
##### 4、io模块源码

-----
##### 5、lang模块源码
* Error：{@code Error}是{@code Throwable}的一个子类，它表示一个合理的应用程序不应该试图捕捉的严重问题。大多数这样的错误都是异常情况。{@code ThreadDeath}错误虽然是一个“正常”条件，但也是{@code error}的一个子类，因为大多数应用程序不应该尝试捕捉它。
-----
##### 6、math模块源码

-----
##### 7、net模块源码

-----
##### 8、nio模块源码

-----
##### 9、rmi模块源码

-----
##### 10、security模块源码

-----
##### 11、sql模块源码

-----
##### 12、text模块源码

-----
##### 13、time模块源码

-----
##### 14、util模块源码
> [util模块源码](src/main/java/util)：
* concurrent：
* function：
* jar：
* logging：
* prefs：
* regex：
* spi：
* stream：
* zip：
* AbstractCollection：此类提供了集合接口的框架实现，以最小化实现此接口所需的工作。
* AbstractList：这个类提供了{@link List}接口的框架实现，以最小化实现这个由“随机访问”数据存储（如数组）支持的接口所需的工作量。对于顺序访问数据（如链表），应优先使用{@link AbstractSequentialList}而不是此类。
* AbstractMap：此类提供Map接口的框架实现，以最小化实现此接口所需的工作。
* AbstractQueue：
* AbstractSequentialList：
* AbstractSet：
* ArrayDeque：
* ArrayList：
* ArrayPrefixHelpers：
* Arrays：
* ArraysParallelSortHelpers：
* Base64：
* BitSet：这个类实现了一个按需增长的位向量。位集的每个组件都有一个{@code boolean}值。{@code BitSet}的位由非负整数索引。可以检查、设置或清除单个索引位。一个{@code位集}可用于通过逻辑与、逻辑包含或和逻辑异或操作修改另一个{@code位集}的内容。
* Calendar：
* Collection：集合层次结构中的根接口。集合表示一组对象，称为其元素。有些集合允许重复元素，而有些则不允许。有些是有序的，有些是无序的。JDK不提供此接口的任何直接实现：它提供更具体的子接口（如Set和List）的实现。此接口通常用于传递集合，并在需要最大通用性的地方对其进行操作。
* Collections：
* ComparableTimSort：
* Comparator：
* Comparators：
* ConcurrentModificationException：
* Currency：
* Date：
* Deque：
* Dictionary：
* DoubleSummaryStatistics：
* DualPivotQuicksort：
* DuplicateFormatFlagsException：
* EmptyStackException：
* Enumeration：实现枚举接口的对象一次生成一系列元素。对nextElement方法的连续调用返回序列的连续元素。
* EnumMap：
* EnumSet：
* EventListener：
* EventListenerProxy：
* EventObject：
* FormatFlagsConversionMismatchException：
* Formattable：
* FormattableFlags：
* Formatter：
* FormatterClosedException：
* GregorianCalendar：
* HashMap：
* HashSet：
* Hashtable：
* IdentityHashMap：
* IllegalFormatCodePointException：
* IllegalFormatConversionException：
* IllegalFormatException：
* IllegalFormatFlagsException：
* IllegalFormatPrecisionException：
* IllegalFormatWidthException：
* IllformedLocaleException：
* InputMismatchException：
* IntSummaryStatistics：
* InvalidPropertiesFormatException：
* Iterator：
* JapaneseImperialCalendar：
* JumboEnumSet：
* LinkedHashMap：
* LinkedHashSet：
* LinkedList：
* List：有序的集合（也称为序列）。此界面的用户可以精确控制每个元素在列表中的插入位置。用户可以通过整数索引（在列表中的位置）访问元素，并在列表中搜索元素。
* ListIterator：
* ListResourceBundle：
* Locale：
* LocaleISOData：
* LongSummaryStatistics：
* Map：
* MissingFormatArgumentException：
* MissingFormatWidthException：
* MissingResourceException：
* NavigableMap：
* NavigableSet：
* NoSuchElementException：
* Objects：此类由{@code static}实用程序方法组成，用于对对象进行操作。这些实用程序包括{@code null}安全或{@code null}容忍的方法，用于计算对象的哈希代码、为对象返回字符串以及比较两个对象。
* Observable：
* Observer：
* Optional：
* OptionalDouble：
* OptionalInt：
* OptionalLong：
* PrimitiveIterator：
* PriorityQueue：
* Properties：
* PropertyPermission：
* PropertyResourceBundle：
* Queue：一种设计用于在处理前保存元素的集合。除了基本的{@linkjava.util.Collection集合Collection}操作，队列提供了额外的插入、提取和检查操作。这些方法都有两种形式：一种是在操作失败时抛出异常，另一种是返回特殊值（根据操作的不同，{@code null}或{@code false}）。后一种形式的insert操作专门设计用于容量受限的{@code Queue}实现；在大多数实现中，insert操作不能失败。
* Random：此类的实例用于生成伪随机数流。该类使用48位种子，该种子使用线性同余公式进行修改。
* RandomAccess：
* RegularEnumSet：
* ResourceBundle：
* Scanner：一个简单的文本扫描器，可以使用正则表达式解析原语类型和字符串。
* ServiceConfigurationError：
* ServiceLoader：
* Set：不包含重复元素的集合。更正式地说，集合不包含一对元素e1和e2，e1等于（e2），最多一个空元素。正如它的名字所暗示的，这个接口为数学集合抽象建模。
* SimpleTimeZone：
* SortedMap：
* SortedSet：
* Spliterator：
* Spliterators：
* SplittableRandom：
* Stack：类表示后进先出（LIFO）对象堆栈。它通过五个操作扩展了类向量，允许将向量视为堆栈。
* StringJoiner：
* StringTokenizer：
* Timer：
* TimerTask：
* TimeZone：
* TimSort：
* TooManyListenersException：
* TreeMap：
* TreeSet：
* Tripwire：
* UnknownFormatConversionException：
* UnknownFormatFlagsException：
* UUID：
* Vector：
* WeakHashMap：基于哈希表实现的Map接口，带有弱键。WeakHashMap中的value在其Key不再正常使用时将自动删除。
