package leetcode.array.easy;

/**
 * Created by liuyi on 2020/6/3 13:32
 */

import java.util.HashMap;
import java.util.Map;

/**
 * 给定一个整数数组 nums 和一个目标值 target，请你在该数组中找出和为目标值的那 两个 整数，并返回他们的数组下标。
 你可以假设每种输入只会对应一个答案。但是，数组中同一个元素不能使用两遍。
 示例:
 给定 nums = [2, 7, 11, 15], target = 9
 因为 nums[0] + nums[1] = 2 + 7 = 9
 所以返回 [0, 1]
 */
public class TwoSum {

    public static void main(String [] args){

        int [] nums ={2, 7, 11, 15};

        int [] result =result=twoSumMap(nums,9);
        if(result!=null){
            for(int index :result){
                System.out.println(index);
            }
        }

    }


    //暴力解法  但是同一个元素会使用两次
    public static int[] twoSum(int[] nums,int target){

        if(nums.length<2){
            return null;
        }

        //遍历数组 从第一个数与后面的数相加判断
        int a,b;
        for(int i =0;i<nums.length;i++){

            a=nums[i];
            for(int j =i+1;j<nums.length;j++){
                b=nums[j];
                if(a+b==target){
                    return  new int[]{i,j};
                }

            }
        }

        return null;
    }

    // 新建一个map,遍历数组
    // 用target减去数组的每一个元素 得到一个期望结果 然后去判断map中有无该值
    // 没有就把该数组元素存在map中 kye:num[i],value:i
    // 有的话就返回当前元素索引与跟map中匹配的数据的索引

    public static int[] twoSumMap(int[] nums,int target){
        Map<Integer,Integer> map = new HashMap<Integer,Integer>();
        for(int i=0;i<nums.length;i++){
            int r =target-nums[i];
            if(map.containsKey(r)){

                return new int[]{i,map.get(r)};

            }else{
                map.put(nums[i],i) ;
            }
        }


        return null;
    }
}
