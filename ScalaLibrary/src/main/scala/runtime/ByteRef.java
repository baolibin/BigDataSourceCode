/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2002-2013, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org                **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */



package runtime;


public class ByteRef implements java.io.Serializable {
    private static final long serialVersionUID = -100666928446877072L;

    public byte elem;
    public ByteRef(byte elem) { this.elem = elem; }
    public String toString() { return Byte.toString(elem); }

    public static ByteRef create(byte e) { return new ByteRef(e); }
    public static ByteRef zero() { return new ByteRef((byte)0); }
}
