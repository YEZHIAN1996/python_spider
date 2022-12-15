# 算法

# 1.sort_slgorithm 冒泡算法

def bubble_sort(nums):
    '''
    冒泡排序
    :param nums: 无序列表
    :return: nums 有序列表
    '''
    for i in range(len(nums)-1):
        for j in range(0, len(nums)-i-1):
            if nums[j] > nums[j+1]:
                nums[j],nums[j+1] = nums[j+1], nums[j]
    return nums

if __name__ == '__main__':
    nums = [8, 7, 12, 3, 2, 11, 10, 6]
    print(bubble_sort(nums))