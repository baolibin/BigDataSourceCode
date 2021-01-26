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
* ：
* ：
* ：
* ：
* ：




