package com.data.tools.yc.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class RbmBinaryEvaluator extends GenericUDAFEvaluator {

    StringObjectInspector stringOI;
    BinaryObjectInspector binaryOI;


    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

        super.init(m, parameters);

        if (Mode.PARTIAL1.equals(m)) {
            stringOI = (StringObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        } else if (Mode.COMPLETE.equals(m)) {
            stringOI = (StringObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        } else if (Mode.PARTIAL2.equals(m)) {
            binaryOI = (BinaryObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
        } else if (Mode.FINAL.equals(m)) {
            binaryOI = (BinaryObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        return new RbmAggregationBuffer();
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        ((RbmAggregationBuffer) aggregationBuffer).reset();
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        if (null == objects || objects.length < 1) {
            return;
        }
        String input = stringOI.getPrimitiveJavaObject(objects[0]);
        ((RbmAggregationBuffer) aggregationBuffer).accumulate(input);

    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        return new BytesWritable(((RbmAggregationBuffer) aggregationBuffer).toBytes());
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
        byte[] input = binaryOI.getPrimitiveJavaObject(o);
        try {
            ((RbmAggregationBuffer) aggregationBuffer).or(RbmAggregationBuffer.fromBytes(input));
        } catch (IOException e) {
            throw new HiveException(input + "can not deserialize to bitmap");
        }

    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        return new Text(((RbmAggregationBuffer) aggregationBuffer).toBase64Str());
    }
}
