import  numpy as np

#@0905————此处的“占优排序”没毛病，完全正确——————只是有前提：目标函数都是求“最小化”
#所以，特别要注意，目标函数最小化（最大化的要转成最小化）

# 对适应度函数（数组）中当前粒子的适应度函数比较（每个分量都比较）
# 如果当前粒子适应度各分量都占优，则为True，至少一个不占优，则为False
# 两两粒子占优比较
# @warn 有点疑问:只比较一个分量就返回True?

#此处应该是：所有fitness分量目标，都是越小越好
# 那只要一个A中的某个元素<B中对应元素，说明A不被占优——————————厉害，这就是快速占优排序，不需要全部比较
#@warn：这条没有被warn，是说明一个逻辑————A至少有一个元素优于B， 则A都不被B占优
#@warn:但是注意————————在此代码中， 我们一定要将各fitness函数全部转换成，求“最小化”————————对于最大化问题，给加个负号，转成最小化
#返回：被占优，则为False; 未被占优，则为True
def compare_ (fitness_curr, fitness_ref):
    for i in range(len(fitness_curr)):
        #原始代码，就是希望目标函数“越小越好”
        if fitness_curr[i] < fitness_ref[i]:
            # if fitness_curr[i] > fitness_ref[i]:  如果这里比较符号为>， 则表示“目标是最大化”的
            return True
    return False

# 当前粒子与data中其他所有粒子占优比较，利用了上面的compare
#@warn 如果当前粒子至少一个“完全占优”当前粒子，则“该粒子”为“劣解”,返回False，否则当前没有粒子对其占优，其加入“初始可行解”
#这个fitness_curr是为了传递给compare_函数
def judge_(fitness_curr, fitness_data, cursor):
    for i in range(len(fitness_data)):
        #自己与自己不做比较，cursor表示自己当前所在行
        if i == cursor:
            continue
        # print("judge111111")
            #这里比较时，貌似应该传入2个参数，此处只有1个？？没有传入fitness_curr
            # 原始代码：if compare_(fitness_data[i]) == False:
        if compare_(fitness_curr, fitness_data[i]) == False:
            # if compare_(fitness_data[i]) == False:
            return False
    return True

#@warn judge_(one_row, N_row):
def judge_one_against_other(one_row, N_row):
    for i in range(len(N_row)):
        if compare_(one_row, N_row[i]) == False:
            return False
    return True

class  Pareto_:
    def __init__(self, in_data, fitness_data):
        self.in_data = in_data
        self.fitness_data = fitness_data
        self.cursor = -1  #初始化游标位置为-1
        # @warn shape???
        #此处为100， 表示100个粒子
        self.len_ = in_data.shape[0]
        self.bad_num = 0 #非优解的个数

    # 获取下一条数据，即第i个粒子的信息：位置（此处为2个元素的数组），适应度信息（此处为2个元素的适应度）
    #也即获取下一个粒子
    def next(self):
        self.cursor = self.cursor + 1
        return self.in_data[self.cursor], self.fitness_data[self.cursor]

    # @warn ？？？
    def hasNext(self):
        return self.len_ > self.cursor + 1 + self.bad_num

    #没得到一个最优解，将其从原始适应度值的数组中删除
    def remove(self):
        #将非占优解从数据集删除， 避免反复与其进行比较
        #删除原2维数组fitness_data里 的 self.cursor行， 因为axis=0，所以表示删除行， 若axis=1，则表示删除列
        self.fitness_data = np.delete(self.fitness_data, self.cursor, axis=0)
        #in_data也要删除对应的cursor所在行，即删除该粒子
        self.in_data = np.delete(self.in_data, self.cursor, axis=0)
        #游标回退一步————因为原fitness_data规模减1，当前游标减一，能保证仍指向原来的fitness_curr
        self.cursor = self.cursor - 1
        #非优解个数+1， @warn ???
        self.bad_num = self.bad_num + 1

#计算pareto非占优解集————初始化粒子个体时，需要计算“非占优解集”,剔除“劣解”，加速搜索
    def pareto(self):
        while(self.hasNext()):
            #获取当前位置的例子信息
            in_curr, fitness_curr = self.next()
            # print(in_curr)
            # print(fitness_curr)
            #判断当前粒子是否“非劣解”
            # 若为“劣解”,直接从当前粒子群中剔除——其为“劣解”,那么比这个更劣的，肯定也会被别的粒子占优，所以该粒子可以被剔除，避免重复计算
            # print("judge")
            # print(self.cursor)
            if judge_(fitness_curr, self.fitness_data, self.cursor) == False:
                # print("remove:" + str(self.cursor))
                self.remove()
        return  self.in_data, self.fitness_data
