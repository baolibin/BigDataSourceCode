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
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：
* ：




