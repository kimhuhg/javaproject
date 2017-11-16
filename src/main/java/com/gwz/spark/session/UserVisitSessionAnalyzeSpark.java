package com.gwz.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.*;
import com.google.common.base.Optional;
import com.gwz.constant.Constants;
import com.gwz.dao.*;
import com.gwz.dao.factory.DAOFactory;
import com.gwz.domain.*;
import com.gwz.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static com.gwz.util.SparkUtils.getSQLContext;

/**
 * 用户访问session分析Spark作业
 * Created by gongwenzhou on 2017/11/14.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION);
        SparkUtils.setMaster(conf);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        SparkUtils.mockData(sc,sqlContext);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //先查询指定任务,再查询出任务的参数
        Long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskId);
        if(null==task){
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String,Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
        //持久化
        sessionid2actionRDD = sessionid2actionRDD.cache();
        //sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sc, sqlContext, sessionid2actionRDD);

        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.cache();

        //获取通过筛选条件的session的访问明细数据RDD
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

        //随机抽取索引
        randomExtractSession(sc, task.getTaskid(),
                filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());

        // 获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(task.getTaskid(), sessionid2detailRDD);

        // 获取top10活跃session
        getTop10Session(sc, task.getTaskid(),
                top10CategoryList, sessionid2detailRDD);


        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 获取top10活跃session
     * @param sc
     * @param taskid
     * @param top10CategoryList
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步：将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList =
                new ArrayList<Tuple2<Long, Long>>();

        for(Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD =
                sc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
                sessionid2detailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<Long, String>> call(
                            Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                        // 计算出该session，对每个品类的点击次数
                        while(iterator.hasNext()) {
                            Row row = iterator.next();

                            if(row.get(6) != null) {
                                long categoryid = row.getLong(6);

                                Long count = categoryCountMap.get(categoryid);
                                if(count == null) {
                                    count = 0L;
                                }

                                count++;

                                categoryCountMap.put(categoryid, count);
                            }
                        }

                        // 返回结果，<categoryid,sessionid,count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

                        for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            long categoryid = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();
                            String value = sessionid + "," + count;
                            list.add(new Tuple2<Long, String>(categoryid, value));
                        }

                        return list;
                    }

                }) ;

        // 获取到to10热门品类，被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
                .join(categoryid2sessionCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, String>> tuple)
                            throws Exception {
                        return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                    }

                });

        /**
         * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<Long, Iterable<String>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();

                        // 定义取topn的排序数组
                        String[] top10Sessions = new String[10];

                        while(iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            // 遍历排序数组
                            for(int i = 0; i < top10Sessions.length; i++) {
                                // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                                if(top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    // 如果sessionCount比i位的sessionCount要大
                                    if(count > _count) {
                                        // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                                        for(int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        // 将i位赋值为sessionCount
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                    // 比较小，继续外层for循环
                                }
                            }
                        }

                        // 将数据写入MySQL表
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        for(String sessionCount : top10Sessions) {
                            if(sessionCount != null) {
                                String sessionid = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                // 将top10 session插入MySQL表
                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskid(taskid);
                                top10Session.setCategoryid(categoryid);
                                top10Session.setSessionid(sessionid);
                                top10Session.setClickCount(count);

                                ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                                top10SessionDAO.insert(top10Session);

                                // 放入list
                                list.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                        }

                        return list;
                    }

                });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionid2detailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

    /**
     *获取top10热门品类
     * @param taskid
     * @param sessionid2detailRDD
     * @return
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> sessionid2detailRDD) {

        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

            private static final long serialVersionUID = 1239624111272405506L;

            @Override
            public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                Long clickCategoryId = row.getLong(6);
                if (clickCategoryId != null) {
                    list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                }

                String orderCategoryIds = row.getString(8);
                if (orderCategoryIds != null) {
                    String[] split = orderCategoryIds.split(",");
                    for (String id : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(id), Long.valueOf(id)));
                    }
                }
                String payCategoryIds = row.getString(10);
                if (payCategoryIds != null) {
                    String[] split = payCategoryIds.split(",");
                    for (String id : split) {
                        list.add(new Tuple2<Long, Long>(Long.valueOf(id), Long.valueOf(id)));
                    }
                }
                return list;
            }

        });
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */
        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         */

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionid2detailRDD);


        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
                payCategoryId2CountRDD);

        /**
         * 自定义二次排序key
         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(

                new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<CategorySortKey, String> call(
                            Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount,
                                orderCount, payCount);

                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }

                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
                sortKey2countRDD.sortByKey(false);

        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);

        for(Tuple2<CategorySortKey, String> tuple: top10CategoryList) {
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }

        return top10CategoryList;

    }

    /**
     * 连接品类RDD与数据RDD
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long,String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {

        JavaPairRDD<Long, Tuple2<Long, com.google.common.base.Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

            private static final long serialVersionUID = 8136435966959398210L;

            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                Long categoryid = tuple._1;
                Optional<Long> optional = tuple._2._2;

                long clickCount = 0L;

                if (optional.isPresent()) {
                    clickCount = optional.get();
                }
                String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                        Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                return new Tuple2<Long, String>(categoryid, value);
            }
        });


        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

            private static final long serialVersionUID = -4617280539414830601L;

            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                Long categoryid = tuple._1;
                String value = tuple._2._1;
                Optional<Long> optional = tuple._2._2;

                long orderCount = 0L;

                if (optional.isPresent()) {
                    orderCount = optional.get();
                }
                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                return new Tuple2<Long, String>(categoryid, value);
            }
        });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if(optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        return tmpMapRDD;

    }

    /**
     * 获取各个品类支付的次数
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            private static final long serialVersionUID = -6073079022178446765L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                return row.get(10) != null;
            }
        })/*.coalesce(100)*/;

        return payActionRDD.mapToPair(new PairFunction<Tuple2<String,Row>, Long, Long>() {

            private static final long serialVersionUID = 3436628859748217716L;

            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                return new Tuple2<Long, Long>(tuple._2.getLong(10),1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {

            private static final long serialVersionUID = -943263003538483033L;

            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

    }

    /**
     * 获取各个品类下单的次数
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            private static final long serialVersionUID = -4533772028992639255L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                return row.get(8) != null;
            }
        })/*.coalesce(100)*/;

        return orderActionRDD.mapToPair(new PairFunction<Tuple2<String,Row>, Long, Long>() {

            private static final long serialVersionUID = 8698428576728795578L;

            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                return new Tuple2<Long, Long>(tuple._2.getLong(8),1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {

            private static final long serialVersionUID = 4852780472031864694L;

            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });

    }

    /**
     * 获取各个品类点击的次数
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {

        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            private static final long serialVersionUID = -5416109298532059626L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                Row row = tuple._2;
                return row.get(6) != null;
            }
        })/*.coalesce(100)*/;

        return clickActionRDD.mapToPair(new PairFunction<Tuple2<String,Row>, Long, Long>() {

            private static final long serialVersionUID = -3942777963385014908L;

            @Override
            public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                return new Tuple2<Long, Long>(tuple._2.getLong(2),1L);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {

            private static final long serialVersionUID = 660488809167961246L;

            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        });


    }

    /**
     * 计算各session范围占比，并写入MySQL
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }

    /**
     * 随机抽取session
     * @param sc
     * @param taskid
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2detailRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步，计算出每天每小时的session数量
         */

        // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair(

                new PairFunction<Tuple2<String,String>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;

                        String startTime = StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }

                });

        /**
         * 思考一下：这里我们不要着急写大量的代码，做项目的时候，一定要用脑子多思考
         *
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value，应该是session聚合数据
         *
         */

        // 得到每天每小时的session数量

        /**
         * 每天每小时的session数量的计算
         * 是有可能出现数据倾斜的吧，这个是没有疑问的
         * 比如说大部分小时，一般访问量也就10万；但是，中午12点的时候，高峰期，一个小时1000万
         * 这个时候，就会发生数据倾斜
         *
         * 我们就用这个countByKey操作，给大家演示第三种和第四种方案
         *
         */

        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();

        for(Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date,<hour,(3,5,20,102)>>

        /**
         * session随机抽取功能
         *
         * 用到了一个比较大的变量，随机抽取索引map
         * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
         * 还是比较消耗内存和网络传输性能的
         *
         * 将map做成广播变量
         *
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();

        for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for(long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if(hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int)(((double)count / (double)sessionCount)
                        * extractNumberPerDay);
                if(hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if(extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for(int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while(extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
                new HashMap<String, Map<String, IntList>>();



        for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry :
                dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();

                for(int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }

        /**
         * 广播变量，很简单
         * 其实就是SparkContext的broadcast()方法，传入你要广播的变量，即可
         */


        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                sc.broadcast(fastutilDateHourExtractMap);

        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */

        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
        // 然后呢，会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> extractSessionids =
                                new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2.iterator();

                        /**
                         * 使用广播变量的时候
                         * 直接调用广播变量（Broadcast类型）的value() / getValue()
                         * 可以获取到之前封装的广播变量
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtractDAO sessionRandomExtractDAO =
                                DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while(iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();

                            if(extractIndexList.contains(index)) {
                                String sessionid = StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                                // 将数据写入MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionid);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }

                            index++;
                        }

                        return extractSessionids;
                    }

                });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(sessionid2detailRDD);
        extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(
                            Iterator<Tuple2<String, Tuple2<String, Row>>> iterator)
                            throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                            Row row = tuple._2._2;

                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskid);
                            sessionDetail.setUserid(row.getLong(1));
                            sessionDetail.setSessionid(row.getString(2));
                            sessionDetail.setPageid(row.getLong(3));
                            sessionDetail.setActionTime(row.getString(4));
                            sessionDetail.setSearchKeyword(row.getString(5));
                            sessionDetail.setClickCategoryId(row.getLong(6));
                            sessionDetail.setClickProductId(row.getLong(7));
                            sessionDetail.setOrderCategoryIds(row.getString(8));
                            sessionDetail.setOrderProductIds(row.getString(9));
                            sessionDetail.setPayCategoryIds(row.getString(10));
                            sessionDetail.setPayProductIds(row.getString(11));

                            sessionDetails.add(sessionDetail);
                        }

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insertBatch(sessionDetails);
                    }

                });
    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String,Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

            private static final long serialVersionUID = -8874013724431682044L;

            @Override
            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                return new Tuple2<String, Row>(tuple._1,tuple._2._2);
            }
        });
        return sessionid2detailRDD;
    }

    /**
     * 过滤session数据并进行聚合统计
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @param sessionAggrStatAccumulator
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {

        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");

        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {

            private static final long serialVersionUID = 1957223567249738949L;

            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggrInfo = tuple._2;

                // 接着，依次按照筛选条件进行过滤
                // 按照年龄范围进行过滤（startAge、endAge）
                if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                        parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    return false;
                }

                // 按照职业范围进行过滤（professionals）
                // 互联网,IT,软件
                // 互联网
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                        parameter, Constants.PARAM_PROFESSIONALS)) {
                    return false;
                }

                // 按照城市范围进行过滤（cities）
                // 北京,上海,广州,深圳
                // 成都
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                        parameter, Constants.PARAM_CITIES)) {
                    return false;
                }

                // 按照性别进行过滤
                // 男/女
                // 男，女
                if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                        parameter, Constants.PARAM_SEX)) {
                    return false;
                }

                // 按照搜索词进行过滤
                // 我们的session可能搜索了 火锅,蛋糕,烧烤
                // 我们的筛选条件可能是 火锅,串串香,iphone手机
                // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                // 任何一个搜索词相当，即通过
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                        parameter, Constants.PARAM_KEYWORDS)) {
                    return false;
                }

                // 按照点击品类id进行过滤
                if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                        parameter, Constants.PARAM_CATEGORY_IDS)) {
                    return false;
                }

                // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                // 进行相应的累加计数

                // 主要走到这一步，那么就是需要计数的session
                sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                        aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);

                return true;

            }

            /**
             * 计算访问时长范围
             * @param visitLength
             */
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                }
            }

            /**
             * 计算访问步长范围
             * @param stepLength
             */
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                }
            }
        });

        return filteredSessionid2AggrInfoRDD;

    }

    /**
     * 聚合信息
     * @param sc
     * @param sqlContext
     * @param sessionid2actionRDD   行为数据RDD
     * @return  session粒度的数据<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {
        //根据session_id进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD.groupByKey();

        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
            private static final long serialVersionUID = -4593359736022879764L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> iterable) throws Exception {
                String session_id = iterable._1;
                Iterator<Row> iterator = iterable._2.iterator();

                StringBuffer searchKeywordsBuffer = new StringBuffer("");
                StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                Long userid = null;
                // session的起始和结束时间
                Date startTime = null;
                Date endTime = null;
                // session的访问步长
                int stepLength = 0;

                while (iterator.hasNext()){
                    Row row = iterator.next();
                    if(null==userid){
                        userid = row.getLong(1);
                    }

                    String keyWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    //如果搜索的关键字存在并且不在Buffer中则添加(点击的商品类目同理)
                    if(null!=keyWord && !searchKeywordsBuffer.toString().contains(keyWord)){
                        searchKeywordsBuffer.append(keyWord + ",");
                    }
                    if(null!=clickCategoryId && !clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                        clickCategoryIdsBuffer.append(clickCategoryId + ",");
                    }

                    Date actionTime = DateUtils.parseTime(row.getString(4));
                    if(startTime==null){
                        startTime = actionTime;
                    }
                    if(endTime==null){
                        endTime = actionTime;
                    }

                    if(actionTime.before(startTime)){
                        startTime = actionTime;
                    }
                    if(actionTime.after(endTime)){
                        endTime = actionTime;
                    }
                    //计算session访问的步长
                    stepLength ++;

                }

                String searchKeyWords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                //计算session的访问时长
                long visitLength = (endTime.getTime()-startTime.getTime()) / 1000;
                String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + session_id + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);

                return new Tuple2<Long, String>(userid,partAggrInfo);
            }
        });

        //查询所有用户的数据
        String sql = " select * from user_info ";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

            private static final long serialVersionUID = 3033829730624418159L;

            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

            private static final long serialVersionUID = -9019552985273937023L;

            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                String partAggrInfo = tuple._2._1;
                Row userInfoRow = tuple._2._2;

                String session_id = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);

                int age = userInfoRow.getInt(3);
                String profession = userInfoRow.getString(4);
                String city = userInfoRow.getString(5);
                String sex = userInfoRow.getString(6);

                String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + profession + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex;

                return new Tuple2<String, String>(session_id,fullAggrInfo);
            }
        });
        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 获取session访问到的对应的action（ROW）
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

            private static final long serialVersionUID = -6586510376868353854L;

            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String, Row>>();
                while(iterator.hasNext()){
                    Row row = iterator.next();
                    String session_id = row.getString(2);
                    list.add(new Tuple2<String, Row>(session_id,row));
                }
                return list;
            }
        });
    }
}
