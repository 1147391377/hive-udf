package com.data.tools.yc.udaf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Base64;

public class RbmAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {
    RoaringBitmap roaringBitmap;

    private  static  final Logger logger = LoggerFactory.getLogger(RbmAggregationBuffer.class);

    public RbmAggregationBuffer(){
        this.roaringBitmap = null;
    }

    public  RbmAggregationBuffer(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    public void accumulate(String input ){
        try{
            if(input != null) {
                if(this.roaringBitmap == null){
                    this.roaringBitmap = new RoaringBitmap();
                }
                BigInteger inputInt = new BigInteger(input);
                int intValue = inputInt.intValue();
                roaringBitmap.add(intValue);
            }
        } catch (NumberFormatException numberFormatException) {
            logger.error(input + "can not be cast to number");
        }
    }

    public void or(RbmAggregationBuffer acc){
        if ( this.roaringBitmap == null) {
            this.roaringBitmap = acc.roaringBitmap;
            return;
        }
        if(acc.roaringBitmap != null) {
            roaringBitmap.or(acc.roaringBitmap);
        }
    }

    public void and(RbmAggregationBuffer acc){
        if ( this.roaringBitmap == null) {
            this.roaringBitmap = acc.roaringBitmap;
            return;
        }
        if(acc.roaringBitmap != null) {
            roaringBitmap.and(acc.roaringBitmap);
        }
    }

    public void xor(RbmAggregationBuffer acc){
        if ( this.roaringBitmap == null) {
            this.roaringBitmap = acc.roaringBitmap;
            return;
        }
        if(acc.roaringBitmap != null) {
            roaringBitmap.xor(acc.roaringBitmap);
        }
    }

    public void andNot(RbmAggregationBuffer acc){
        if ( this.roaringBitmap == null) {
            this.roaringBitmap = acc.roaringBitmap;
            return;
        }
        if(acc.roaringBitmap != null) {
            roaringBitmap.andNot(acc.roaringBitmap);
        }
    }

    public void reset(){
        roaringBitmap = null;
    }

    public long estimateCardinality(){
        if(this.roaringBitmap == null){
            this.roaringBitmap = new RoaringBitmap();
        }
        return this.roaringBitmap.getLongCardinality();
    }

    public byte[] toBytes(){
        if(this.roaringBitmap == null){
            this.roaringBitmap = new RoaringBitmap();
        }
        int size = roaringBitmap.serializedSizeInBytes();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        roaringBitmap.serialize(buffer);
        return buffer.array();
    }

    public static RbmAggregationBuffer fromBytes(byte[] bytes) throws IOException {
        return  new RbmAggregationBuffer(bytesToRbm(bytes));
    }

    public String toBase64Str(){
        byte[] bytes = toBytes();
        String finalStr = new String(Base64.getEncoder().encode(bytes));
        return finalStr;
    }

    public static  RbmAggregationBuffer fromBase64Str(String base64Str) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64Str);
        return  new RbmAggregationBuffer(bytesToRbm(bytes));

    }

    public static  RoaringBitmap bytesToRbm(byte[] bytes) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(inputStream);
        RoaringBitmap roaringBitmap1 = new RoaringBitmap();
        roaringBitmap1.deserialize(in);
        return  roaringBitmap1;
    }

    public static RoaringBitmap base64ToRbm(String base64Str) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64Str);
        return  bytesToRbm(bytes);
    }

    @Override
    public int estimate() {
        return toBytes().length;
    }
}
