#### Scala Library和Scala Library-aux源码阅读
    ScalaLibrary和Scala Library-Aux模块源码阅读，版本2.11.8。

-----
##### 1、核心Scala Library-Aux功能模块源码
> [核心Scala Library-Aux功能模块源码](src/main/scala/libiary-aux)：
* Any：Class`Any`是Scala类层次结构的根。Scala执行环境中的每个类都直接或间接地继承自这个类。
* AnyRef：类“AnyRef”是所有“引用类型”的根类。除值类型以外的所有类型都从此类派生。
* Nothing：`Nothing`是其他所有类型的子类型（包括[[scala.Null]]);不存在此类型的“实例”。
* Null： `Null`是所有引用类型的子类型；它的唯一实例是'Null'引用。

-----
##### 2、核心Scala Library功能模块源码
> [核心ScalaLibrary功能模块源码](src/main/scala)： 核心Scala类型。它们在没有显式导入的情况下始终可用。
* AnyVal：`AnyVal`是所有“值类型”的根类，它描述在基础主机系统中未实现为对象的值。
* AnyValCompanion：基本类型伴随类的一种常见的超类型。
* App：“App”特性可用于快速将对象转换为可执行程序。
* Array：对数组进行操作的实用方法。
* Boolean：`Boolean`（相当于Java的“Boolean”基元类型）是[[[scala.AnyVal]]子类型。
* Byte：`Byte`，一个8位有符号整数（相当于Java的Byte基元类型）是[[scala.AnyVal]]子类型。
* Char：`Char`，一个16位无符号整数（相当于Java的“Char”原语类型）是[[scala.AnyVal]]子类型。
* Cloneable：扩展这个特性的类可以跨平台（Java，.NET）克隆。
* deprecated：一种注释，指明某个定义已被否决。对成员的访问将生成一个不推荐使用的警告。
* deprecatedInheritance：不推荐使用指定从类继承的注释。
* deprecatedName：一种注释，将应用该注释的参数的名称指定为已弃用。在命名参数中使用该名称将生成弃用警告。
* deprecatedOverriding：指定重写成员的注释已弃用。
* Double：`Double`，一个64位的IEEE-754浮点数（相当于Java的“Double”原语类型）是[[scala.AnyVal]]子类型。
* Dynamic：支持动态调用的标记特性。此特性的实例“x”允许对任意方法名“meth”和参数列表“args”调用“x.meth（args）”，并允许对任意字段名“field”访问“x.field”。
* Enumeration：定义特定于枚举的有限值集。通常，这些值枚举了所有可能的形式，并为case类提供了一个轻量级的替代方案。
* Equals：包含相等操作的接口。类“AnyRef”中唯一不存在的方法是“canEqual”。
* Float：`Float`，一个32位的IEEE-754浮点数（相当于Java的“Float”原语类型），是[[scala.AnyVal]]子类型。
* Function：定义高阶函数式程序设计实用方法的模块。
* Function0：参数为0的函数。
* inline：
* Int：`Int`，一个32位有符号整数（相当于Java的“Int”原语类型）是[[scala.AnyVal]]子类型。
* Long：`Long`，一个64位有符号整数（相当于Java的“Long”原语类型）是[[scala.AnyVal]]子类型。
* MatchError：此类实现了每当对象与模式匹配表达式的任何模式都不匹配时抛出的错误。
* Mutable：可变数据结构（如可变集合）的标记特征。
* native：本机方法的标记。
* noinline：方法上的注释，禁止编译器内联该方法，无论内联看起来多么安全。
* NotImplementedError：抛出此异常可以临时替换仍有待实现的方法体。
* NotNull：不允许为空的事物的标记特征。
* Option：表示可选值。“Option”的实例可以是$some的实例，也可以是$none的对象。
* PartialFunction：“PartialFunction[A，B]”类型的偏函数是一元函数，其中域不一定包含“A”类型的所有值。函数“isDefinedAt”允许动态测试值是否在函数的域中。
* Predef：“Predef”对象提供的定义可以在所有Scala编译单元中访问，而无需显式限定。
* Product：所有products的基本特征，标准库中至少包括[[scala.Product1]]到[[scala.Product22]]，也包括它们的子类[[scala.Tuple1]]到[[scala.Tuple22]]。此外，所有case类都使用综合生成的方法实现“Product”。
* Product1：Product1是一个元素的笛卡尔积。
* Proxy：这个类实现了一个简单的代理，它将所有对类Any中定义的非final公共方法的调用转发给另一个对象self。
* remote：一种注释，将应用它的类指定为可远程处理的。
* Responder：此对象包含用于生成响应程序的实用方法。
* Serializable：扩展这个特性的类可以跨平台（Java，.NET）序列化。
* SerialVersionUID：用于指定可序列化类的“static SerialVersionUID”字段的批注。
* Short：`Short`，一个16位有符号整数（相当于Java的“Short”原语类型）是[[scala.AnyVal]]子类。
* Specializable：特殊类型的同伴的一个普通超类型。不应在用户代码中扩展。
* specialized：注释应该自动专门化代码的类型参数。
* StringContext：此类提供了执行字符串插值的基本机制。
* Symbol：这个类提供了一种简单的方法来获取相等字符串的唯一对象。
* throws：用于指定方法引发的异常的注释。
* transient：
* Tuple1：一个由1个元素组成的元组；一个[[scala.Product1]]的规范化表示。
* unchecked：一种注释，用于指定附加编译器检查时不应考虑带注释的实体。
* UninitializedError：此类表示未初始化的变量/值错误。
* UninitializedFieldError：此类实现了在初始化字段之前使用字段时抛出的错误。
* Unit：`Unit`是一个[[scala.AnyVal]]子类型。只有一个“Unit”类型的值，`（）`，并且它不由基础运行时系统中的任何对象表示。返回类型为“Unit”的方法类似于声明为“void”的Java方法。
* volatile：


