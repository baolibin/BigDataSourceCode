/*
 * Copyright (c) 1996, Oracle and/or its affiliates. All rights reserved.
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

package java.beans;

/**
 * customizer类为定制目标javabean提供了一个完整的自定义GUI。
 * <p>
 * A customizer class provides a complete custom GUI for customizing
 * a target Java Bean.
 * <p>
 * Each customizer should inherit from the java.awt.Component class so
 * it can be instantiated inside an AWT dialog or panel.
 * <p>
 * Each customizer should have a null constructor.
 */

public interface Customizer {

	/**
	 * Set the object to be customized.  This method should be called only
	 * once, before the Customizer has been added to any parent AWT container.
	 * @param bean The object to be customized.
	 */
	void setObject(Object bean);

	/**
	 * Register a listener for the PropertyChange event.  The customizer
	 * should fire a PropertyChange event whenever it changes the target
	 * bean in a way that might require the displayed properties to be
	 * refreshed.
	 * @param listener An object to be invoked when a PropertyChange
	 *                 event is fired.
	 */
	void addPropertyChangeListener(PropertyChangeListener listener);

	/**
	 * Remove a listener for the PropertyChange event.
	 * @param listener The PropertyChange listener to be removed.
	 */
	void removePropertyChangeListener(PropertyChangeListener listener);

}
