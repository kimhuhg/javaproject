package com.gwz.spark.session;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by root on 2017/11/15.
 */
public class CategorySortKey implements Ordered<CategorySortKey>,Serializable {

    private static final long serialVersionUID = -6007890914324789180L;



    private long clickCount;
    private long orderCount;
    private long payCount;

    @Override
    public boolean $less(CategorySortKey other) {
        if(this.clickCount < other.getClickCount()){
            return true;
        }else if(this.clickCount == other.clickCount
                && this.orderCount < other.getOrderCount()){
            return true;
        }else if(this.clickCount == other.getClickCount()
                && this.orderCount == other.getOrderCount()
                && this.payCount < other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey other) {
        if(this.clickCount > other.getClickCount()){
            return true;
        }else if(this.clickCount == other.clickCount
                && this.orderCount > other.getOrderCount()){
            return true;
        }else if(this.clickCount == other.getClickCount()
                && this.orderCount == other.getOrderCount()
                && this.payCount > other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey other) {
        if(this.$less(other)){
            return true;
        }else if(this.clickCount == other.getClickCount()
                && this.orderCount == other.getOrderCount()
                && this.payCount == other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey other) {
        if(this.$greater(other)){
            return true;
        }else if(this.clickCount == other.getClickCount()
                && this.orderCount == other.getOrderCount()
                && this.payCount == other.getPayCount()){
            return true;
        }
        return false;
    }

    @Override
    public int compare(CategorySortKey other) {
        return finalCompare(other);
    }

    private int finalCompare(CategorySortKey other) {
        if(this.clickCount - other.getClickCount()!= 0){
            return (int) (clickCount - other.getClickCount());
        }else if(this.orderCount - other.getOrderCount() != 0){
            return (int)(this.orderCount - other.getOrderCount());
        }else if(this.payCount - other.getPayCount() != 0){
            return (int)(this.payCount - other.getPayCount() );
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey other) {
        return finalCompare(other);
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }
}
