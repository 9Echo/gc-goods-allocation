# -*- coding: utf-8 -*-
# @Time    : 2020/03/05
# @Author  : shaoluyu
# Load libraries
import pandas
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
from sklearn import model_selection
from sklearn.externals import joblib
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
# Load dataset
url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
names = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']
dataset = pandas.read_csv(url, names=names)
# 查看行列情况
print(dataset.shape)
# 取前20条
print(dataset.head(20))
# 对每个属性的统计摘要，包含了数量、平均值、最大值、最小值，还有一些百分位数值。
print(dataset.describe())
# 属于每个类别下的行的数量
print(dataset['class'].value_counts())
# 考虑到输入变量都是数字，我们可以为每个输入变量创建箱线图。
dataset.plot(kind='box', subplots=True, layout=(2, 2), sharex=False, sharey=False)
plt.show()
# 我们也可以为每个输入变量创建一个直方图以了解它们的分布状况
dataset.hist()
# 我们看看全部属性对的散点图，这有助于我们看出输入变量之间的结构化关系
plt.show()
scatter_matrix(dataset)
plt.show()
# Split-out validation dataset
array = dataset.values
# 样本数据
X = array[:, 0:4]
# 样本标签
Y = array[:, 4]
# 测试集占比
validation_size = 0.20
# 随机数种子，种子不同，每次采的样本不一样；
# 种子相同，采的样本不变（random_state不取，采样数据不同，
# 但random_state等于某个值，采样数据相同，取0的时候也相同，这可以自己编程尝试下，不过想改变数值也可以设置random_state = int(time.time())）
seed = 7
# 评估度量  准确度评估
scoring = 'accuracy'
X_train, X_validation, Y_train, Y_validation = model_selection.train_test_split(X, Y, test_size=validation_size,
                                                                                random_state=seed)
models = []
models.append(('LR', LogisticRegression()))
models.append(('LDA', LinearDiscriminantAnalysis()))
models.append(('KNN', KNeighborsClassifier()))
models.append(('CART', DecisionTreeClassifier()))
models.append(('NB', GaussianNB()))
models.append(('SVM', SVC(gamma='scale')))
# evaluate each models in turn
results = []
names = []
# model_selection.KFold  交叉验证
# n_splits：折叠数量，表示划分成几份。 必须至少2。版本0.20更改：n_splits默认值将在v0.22中从3更改为5。
# shuffle：布尔值，可选。 是否在划分之前对每个类的样本进行洗牌。
# random_state：随即种子。int，RandomState实例或None，默认=无。
# 如果是int，则random_state是随机数生成器使用的种子; 如果是RandomState实例，则random_state是随机数生成器;
# 如果为None，则随机数生成器是np.random使用的RandomState实例。 在shuffle == True时使用。
for name, model in models:
    kfold = model_selection.KFold(n_splits=10, random_state=seed)
    cv_results = model_selection.cross_val_score(model, X_train, Y_train, cv=kfold, scoring=scoring)
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
    print(msg)
# 模型准确率图形化
fig = plt.figure()
fig.suptitle('Algorithm Comparison')
ax = fig.add_subplot(111)
plt.boxplot(results)
ax.set_xticklabels(names)
plt.show()
# Make predictions on validation dataset
knn = KNeighborsClassifier()
knn.fit(X_train, Y_train)
predictions = knn.predict(X_validation)
print(accuracy_score(Y_validation, predictions))
print(confusion_matrix(Y_validation, predictions))
print(classification_report(Y_validation, predictions))
joblib.dump(knn, 'knn.model')
