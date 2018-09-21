import numpy as np
import random

#外部解集更新——————@cl"0910，此处略复杂，参见粒子群第三章，然后修改————此处的全局最优解是基于密度的
class mesh_crowd(object):
    def __init__(self, curr_archiving_in, curr_archiving_fit, mesh_div, min_, max_, particles):
        self.curr_archiving_in = curr_archiving_in
        self.curr_archiving_fit = curr_archiving_fit
        self.mesh_div = mesh_div
        #@cl初始外部解集的个数num_
        self.num_ = self.curr_archiving_in.shape[0]
        self.particles = particles
        #@cl初始化解集的id，num_行的向量
        self.id_archiving = np.zeros(self.num_)
        #@cl拥挤度的解集，num_行的向量
        self.crowd_archiving = np.zeros((self.num_))
        #@cl概率的解集，num_行的向量
        self.probability_archiving = np.zeros((self.num_))
        #@cl：gbest_in（全局最优解的坐标，粒子个数为行，目标函数个数为列，二维矩阵
        self.gbest_in = np.zeros((self.particles, self.curr_archiving_in.shape[1]))
        # @cl：gbest_fitness（全局最优解的目标值矩阵，粒子个数为行，目标函数个数为列，二维矩阵
        self.gbest_fit = np.zeros((self.particles, self.curr_archiving_fit.shape[1]))
        self.min_ = min_
        self.max_ = max_

    def cal_mesh_id(self, in_):
        id_ = 0
        print("curr_ar")
        #不应该是curr_archiving_in的shape[1], 而应该是curr_archiving_fit的shape[1], 二者的shape[0]一定相同，但是shape[1]却不一样，前者是所有决策变量的个数，后者是目标函数个数
        #而且：拥挤度，是根据目标空间的网格确定
        # for i in range(self.curr_archiving_in.shape[1]):
        for i in range(self.curr_archiving_fit.shape[1]):
            #明白这个意思了，这里以mesh_div为进制数，  假设mesh_div为10 ， 如果粒子是5维的，id = 13427， 则表示粒子所在网格的位置为（1,3,4,2,7），其中每个网格都被分成10等分，从小往大依次为1~10——其实这里只是记号，跟进制无关，只是解析id成坐标时，则需要
            #这里使用id计算，后面需要解析，这种id可以节省存储空间，很棒，但后面需要解析回来

            #@warn， 此处网格好跟self.num_没关系，此处self.num_应替换为self.mesh_div,而且int没毛病，相当于地板函数，int(1.1)=1
            #原代码如下——
            # id_dim = int((in_[i] - self.min_[i]) * self.num_ / (self.max_[i] - self.min_[i]))
            id_dim = int((in_[i] - self.min_[i]) * self.mesh_div / (self.max_[i] - self.min_[i]))

            print("id_dim "+str(i)+"is"+str(id_dim))
            id_ = id_ + id_dim * (self.mesh_div ** i)
            print(id_)
        return id_

    #计算粒子所在的网格id
    def divide_archiving(self):
        for i in range(self.num_):
            #此处计算拥挤度和网格id，应该是根据“适应度函数”空间，而不是粒子位置的空间
            # self.id_archiving[i] = self.cal_mesh_id(self.curr_archiving_in[i])
            self.id_archiving[i] = self.cal_mesh_id(self.curr_archiving_fit[i])

    #计算拥挤度——即具有相同网格id(id_archving一样的粒子，处于同一网格）的粒子集合，他们对应的那个网格的拥挤度 = 其个数
    def get_crowd(self):
        #np.linspace(a,b,c)表示生成一维数组， 下限a，上限b， c-1为等分数，  返回等间距的位置：
        #如np.linspace(0,2,4)  就是将[0,2] 区间 3等分，得到array([ 0.        ,  0.66666667,  1.33333333,  2.        ])
        index_ = (np.linspace(0, self.num_ - 1, self.num_)).tolist()
        #@warn 代码修改
        #原代码：index_ = map(int, index_)
        index_ = list(map(int, index_))
        #注意：此处是将索引取整数了，即[1.1,2.6] 会变成 [1, 2]
        #map用于可迭代对象，如a = [1,2,3] , b = map(str,a)，迭代器b转为list--list(b)，得到['1','2','3']
        #见：https://www.cnblogs.com/lyy-totoro/p/7018597.html

        #此处使用了“栈”的方式，即list.remove方法
        #逻辑： 先选择index_列表的第1个元素， 比较index_中剩余其它元素和其比较网格id，
            # 如果id相同，则把该元素放入到index_same中（index_same初始化时都放入index_[0],即被比较的元素
            # index_same的长度，即为该粒子的长度
            # 对于每个在index_same中的例子：
            #     更新self.crow_archiving这个列表（该列表的索引对应的就是粒子的顺序），其每个元素都是刚才的“拥挤度”————也即每个粒子的拥挤度
            #     从列表index_中去掉所有index_same中出现过的元素,进入新一轮的拥挤度运算
        while(len(index_) > 0):
            #用于存放
            index_same = [index_[0]]
            for i in range(1, len(index_)):
                if self.id_archiving[index_[0]] == self.id_archiving[index_[i]]:
                    #注意：此处index_same添加的是index_[i], 即粒子的编号（应该就是0一直到N，N表示N个粒子）
                    index_same.append(index_[i])
            number_ = len(index_same)
            #对每个粒子i的编号下，更新其拥挤度——————粒子i:列表index_的索引顺序， index_[i]:粒子i的编号 ， crow_archiving[index_[i]:粒子i编号的对应“拥挤度”——后二者是一一对应关系
            for i in index_same:
                self.crowd_archiving[i] = number_
                index_.remove(i)

#此类:选择全局最优粒子引导者————
#内部的概率是：  外部储备集中，某个粒子被选成粒子A的“全局引导者”的概率
class get_gbest(mesh_crowd):
    def __init__(self, curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_, particles):
        # @warn 修改——————
        super(get_gbest, self).__init__(curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_, particles)
        self.divide_archiving()
        self.get_crowd()
        #@warn 修改——————
        # self.gbest_index = 0

    #@warn 此处被写死了，需要修改
    #@warn 注意，如果一个函数没有关键字， 则其返回的是None， 相当于java中的void, 这时候，一般用于更新类里的成员变量值
    #@此处满足： 拥挤度越大，粒子存在于解集中的概率越小， 下一次迭代时，其被淘汰的概率就很大
    #此处的粒子间a和b的相对概率 Pr = [crow(b) / crowd(a)] ^ 3   ——————一个拥挤度为10的粒子 ，是1个拥挤度为1的粒子出现的概率的 Pr = 1/1000
    #即“拥挤度函数”的倒数，——————a比b拥挤1000倍，则a出现的概率是b的1/1000
    def get_probability(self):
        for i in range(self.num_):
            #解集被选中成生成下一代的概率？写个灵活点的
            #即np中的array的每个元素的3次方
            #此处计算方式为：
            #假设粒子1 的拥挤度为10， 则其“概率”= 10 /10**3 = 0.01, 但这还不是真正的概率，因为不满足“求和为1”的条件
            self.probability_archiving = 10.0 / (self.crowd_archiving ** 3)
            #使概率和为1，生成真正的概率
            self.probability_archiving = self.probability_archiving / np.sum(self.probability_archiving)

    #@warn ？？？
    #此处的全局粒子是“随机的”————不是基于“基于最小角度的”
    #@warn 考虑重写————在当前储备集中， 与粒子角度最小的，则为该粒子的全局引导者
    def get_gbest_index(self):
        random_pro = random.uniform(0.0, 1.0)
        for i in range(self.num_):
            if random_pro <= np.sum(self.probability_archiving[0: i + 1]):
                return i

    #利于上面某个粒子获取全局粒子引导者的索引，可以获得当前全局粒子
    #返回粒子的“gbest_in,gbest_fit”矩阵， 每个粒子i的全局引导者的位置即为gbest_in[i]
    def get_gbest(self):
        self.get_probability()
        for i in range(self.particles):
            #此处，对于每个粒子都获取一个gbest_index索引， 然后更新gbest_in矩阵，其和粒子的索引位置，也是一样的
            gbest_index = self.get_gbest_index()
            print("gbest_index is " + str(gbest_index))
            #根据gbest_index确定全局粒子引导者，在外部储备集的位置，然后得到“该全局粒子”的位置、适应度矩阵
            self.gbest_in[i] = self.curr_archiving_in[gbest_index]
            self.gbest_fit[i] = self.curr_archiving_fit[gbest_index]
        return self.gbest_in, self.gbest_fit

#@warn 修改
#原代码报错：TypeError: super(type, obj): obj must be an instance or subtype of type————是说clear_archiving类没有继承get_best类‘

#此类————用于清理“外部储备集”————当新的粒子是非占优解时，加入到外部储备集中，由于其规模固定，所以必须从其中再删掉一个
class clear_archiving(mesh_crowd):
# class clear_archiving(get_gbest):
    #@warn 修改 ——————增加了gbest_index属性，供其子类使用
    def __init__(self, curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_, particles ):
        #@warn 修改——这里原来没有“位置参数”，故报错，我填充之：TypeError: __init__() missing 1 required positional argument: 'particles'
        # super(get_gbest, self).__init__(curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_)
        super(clear_archiving, self).__init__(curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_, particles)
        # super(clear_archiving, self).__init__(curr_archiving_in, curr_archiving_fit, mesh_div_num, min_, max_, particles)
        self.divide_archiving()
        self.get_crowd()

    #新外部储备集中，粒子被删掉的概率——————仍根据拥挤度， 不过相对概率变小了，按平方计算——————遵循：同一网格的粒子被删概率相同， 不同网格粒子被删除概率和拥挤度的平方成反比
    def get_probability(self):
        for i in range(self.num_):
            #@warn ??? 计算解集概率？是否固定写法？
            self.probability_archiving = self.crowd_archiving ** 2

    #此处获取被删除的粒子的概率
    #逻辑和全局粒子引导者选择相似的逻辑，也是随机的
    #返回要被删除的外部储备集中部分解的索引的列表
    def get_clear_index(self):
        len_clear = (self.curr_archiving_in).shape[0] - self.thresh
        clear_index = []
        while(len(clear_index) < len_clear):
            random_pro = random.uniform(0.0, np.sum(self.probability_archiving))
            for i in range(self.num_):
                if random_pro <= np.sum(self.probability_archiving[0: i + 1]):
                    if i not in clear_index:
                        clear_index.append(i)
        return  clear_index

    def clear_(self, thresh):
        self.thresh = thresh
        #@warn ??? 未出现此变量？？？,下文也没有用到
        # self.archiving_size = archiving_size
        self.get_probability()

        #@warn 代码修改处
        # clear_index = get_clear_index()
        clear_index = self.get_clear_index()

        #@warn这两行代码有问题，gbest_index处，貌似全局都没有该变量，推断为如下
        print([ i for i in self.curr_archiving_in])

        #安装np的语法，我这个应该是没错的，得到clear_index这个列表后，直接删除所有curr_archiving_in中的对应行（clear_index是要被删除的行数）
        self.curr_archiving_in = np.delete(self.curr_archiving_in, clear_index, axis=0)
        self.curr_archiving_fit = np.delete(self.curr_archiving_fit, clear_index, axis=0)
        #原代码如下：
        # self.curr_archiving_in = np.delete(self.curr_archiving_in[gbest_index], clear_index, axis=0)
        # self.curr_archiving_fit = np.delete(self.curr_archiving_fit[gbest_index], clear_index, axis=0)
        #
        # self.curr_archiving_in = np.delete(self.curr_archiving_in[self.get_gbest_index()], clear_index, axis=0)
        # self.curr_archiving_fit = np.delete(self.curr_archiving_fit[self.get_gbest_index()], clear_index, axis=0)
        return self.curr_archiving_in, self.curr_archiving_fit
