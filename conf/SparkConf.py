class BaseConf:
    MASTER = "local[*]"


class SearchItemsConf(BaseConf):
    APP_NAME = "python_SearchOnlineAnalysis"


class ItemDetailConf(BaseConf):
    APP_NAME = "python_ItemDetailAnalysis"


class ShopCarConf(BaseConf):
    APP_NAME = "python_ShopCarAnalysis"
