package leetcode.binarysearch;

/**
 * Created by liuyi on 2020/6/3 15:51
 */

/**
 * 要求数据结构提前排好序
 * 并且可以随机访问 不需要从头到尾遍历
 * 实现方式：1.循环 2.递归
 * 时间复杂度一样：O(log2 N)
 */
public class BinarySearchDemo {

    public static void main(String[] args){

        int[] nums = {1,2,3,4,5,6,7};
        int key = 8;
        System.out.println(binarySearch(nums,key));
        System.out.println(binarySearch2(nums,key,0,6));

    }


    /**
     * 循环实现
     * @param nums
     * @param key 目标数据
     * @return 返回要查找数字的索引
     */
    public static int binarySearch(int[] nums,int key){

        int left = 0;
        int right = nums.length-1;
        int mid = (left+right)/2;

        if(key<nums[left]||key>nums[right]){
            return -1;
        }

        while(left<=right){
            //判断中间点的数与key的大小关系
            if(nums[mid]==key){
                return mid;
            }else if(nums[mid]>key) {
                right = mid -1;
                mid =(left+right)/2;
            }else if(nums[mid]<key){
                left =mid +1;
                mid =(left+right)/2;
            }

        }

        return -1;
    }

    /**
     * 递归实现
     * @param nums
     * @param key
     * @param left
     * @param right
     */
    public static int binarySearch2(int[] nums,int key,int left,int right){

        //递归终止条件
        if(left>right ||nums[left]>key||nums[right]<key){
            return -1;
        }

        int mid =(left+right)/2;

        if(nums[mid]==key){
            return mid;
        }else if(nums[mid]>key){
            binarySearch2(nums,key,left,mid-1);
        }else if(nums[mid]<key){
            binarySearch2(nums,key,mid+1,right);
        }


        return -1;

    }
}
