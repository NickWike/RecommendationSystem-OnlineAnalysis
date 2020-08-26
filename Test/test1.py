import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D


fig = plt.figure()
ax1 = Axes3D(fig)


def f(x1, x2):
    return x1 ** 2 + x2 ** 2


def f_x1(x1):
    return 2 * x1


def f_x2(x2):
    return 2 * x2


def f_x(vector):
    return np.array([f_x1(vector[0]), f_x2(vector[1])])


def gradient_descent(start, a, func):
    result = np.array([np.array(start)])
    print(result)
    for i in range(1, 10000):
        pre_vector = result[i - 1]
        change_value = pre_vector - a * func(pre_vector)
        result = np.row_stack((result, change_value))
    return result


x = np.linspace(-4, 4, 50)

y = np.linspace(-4, 4, 50)

g_x = gradient_descent([4, 4], 0.1, f_x)

# print(np.column_stack((g_x, f(g_x[:, 0], g_x[:, 1]))))

x, y = np.meshgrid(x, y)

z = y ** 2 + x ** 2

ax1.plot_surface(x, y, z, rstride=1,  # row 行步长
                 cstride=1,  # colum 列步长
                 cmap=plt.cm.cool)  # 渐变颜色

ax1.step(g_x[:, 0], g_x[:, 1], f(g_x[:, 0], g_x[:, 1]), marker="x", c="r", where="post")

plt.show()

# import requests
# import time
# import random
#
# blog_urls = ['https://blog.csdn.net/qq_42359956/article/details/105343806',
#  'https://blog.csdn.net/qq_42359956/article/details/105315546',
#  'https://blog.csdn.net/qq_42359956/article/details/105242127',
#  'https://blog.csdn.net/qq_42359956/article/details/105227542',
#  'https://blog.csdn.net/qq_42359956/article/details/105199357',
#  'https://blog.csdn.net/qq_42359956/article/details/105187150',
#  'https://blog.csdn.net/qq_42359956/article/details/104729796',
#  'https://blog.csdn.net/qq_42359956/article/details/104426445',
#  'https://blog.csdn.net/qq_42359956/article/details/104234877',
#  'https://blog.csdn.net/qq_42359956/article/details/102924324',
#  'https://blog.csdn.net/qq_42359956/article/details/102836106',
#  'https://blog.csdn.net/qq_42359956/article/details/102825140',
#  'https://blog.csdn.net/qq_42359956/article/details/102806808',
#  'https://blog.csdn.net/qq_42359956/article/details/87704205',
#  'https://blog.csdn.net/qq_42359956/article/details/87625522',
#  'https://blog.csdn.net/qq_42359956/article/details/87297908',
#  'https://blog.csdn.net/qq_42359956/article/details/87297627',
#  'https://blog.csdn.net/qq_42359956/article/details/87297261',
#  'https://blog.csdn.net/qq_42359956/article/details/84544672',
#  'https://blog.csdn.net/qq_42359956/article/details/84504058',
#  'https://blog.csdn.net/qq_42359956/article/details/81275127',
#  'https://blog.csdn.net/qq_42359956/article/details/81228337']
#
# user_agents = ["Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
#                "Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166  Safari/535.19",
#                "Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
#                "Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
#                "Mozilla/5.0 (Android; Tablet; rv:14.0) Gecko/14.0 Firefox/14.0",
#                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0",
#                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20130331 Firefox/21.0",
#                "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
#                "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19",
#                "Mozilla/5.0 (Linux; Android 4.1.2; Nexus 7 Build/JZ054K) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
#                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
#                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Ubuntu/11.10 Chromium/27.0.1453.93 Chrome/27.0.1453.93 Safari/537.36",
#                "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36",
#                "Mozilla/5.0 (iPhone; CPU iPhone OS 6_1_4 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) CriOS/27.0.1453.10 Mobile/10B350 Safari/8536.25",
#                "Mozilla/5.0 (compatible; WOW64; MSIE 10.0; Windows NT 6.2)",
#                "Mozilla/4.0 (Windows; MSIE 6.0; Windows NT 5.2)",
#                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
#                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
#                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
#                 "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
#                 "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
#                 "Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3",
#                 "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
#                 "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
#                 "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
#                 "Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
#                 "Mozilla/4.0 (compatible; MSIE 7.0; Windows Phone OS 7.0; Trident/3.1; IEMobile/7.0; LG; GW910)",
#                 "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; SAMSUNG; SGH-i917)",
#                 "Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)"
#                ]
#
# headers ={"User-Agent": ""}
# click_times = 4
# cnt = 0
# total = len(blog_urls) * click_times
#
# for i in range(click_times):
#     random.shuffle(blog_urls)
#     for url in blog_urls:
#         headers["User-Agent"] = random.choice(user_agents)
#         requests.get(url, headers=headers)
#         cnt += 1
#         n = (cnt / total)*100
#         print("\r[" + ">" * int(n) + " " * int(100 - n) + "]", end="")
#         print("%.2f%%" % n+"cnt:%d" % cnt, end="")
#         time.sleep(random.randint(120, 240))
#     time.sleep(random.randint(240, 360))
