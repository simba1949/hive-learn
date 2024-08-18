package vip.openpark.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author anthony
 * @version 8/18/2024
 * @since 8/18/2024 4:26 PM
 */
public class MyLength extends GenericUDF {
	/**
	 * 在执行函数之前，会调用这个方法，用于初始化
	 * 可以做参数类型、参数个数的校验
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
		if (objectInspectors.length != 1) {
			throw new UDFArgumentException("参数个数必须是1");
		}
		ObjectInspector objectInspector = objectInspectors[0];
		if (objectInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException("参数类型必须是基本类型");
		}
		// 强转成 PrimitiveObjectInspector
		PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
		if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("参数类型必须是字符串");
		}

		return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
	}

	/**
	 * 执行函数
	 * 每处理一行数据都会调用一次这个方法
	 */
	@Override
	public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
		if (deferredObjects.length != 1) {
			return null;
		}
		// 获取参数值
		Object object = deferredObjects[0].get();
		if (object == null) {
			return 0;
		}

		return object.toString().length();
	}

	/**
	 * 获取在执行计划时，需要展示给用户的描述信息
	 */
	@Override
	public String getDisplayString(String[] strings) {
		return null;
	}
}