import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров

    Args:
        page (str): Идентификатор страницы c результатами.
        campaign_id (str): ID кампании в API и магазина в кабинете.
        access_token (str): Авторизационный токен.

    Returns:
        dict: Данные о товарах, полученные с Яндекс Маркета.

    Examples:
        >>> get_product_list(page, campaign_id, access_token)
        {"status":"OK","result":{"paging":{"nextPageToken":"str","prevPageToken":"str"}, ...}}}

        >>> get_product_list(wrong_page, campaign_id, access_token)
        {"status":"OK","errors":[{"code":"str","message":"str"}]}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров на Яндекс Маркете.

    Args:
        stocks (list): Данные о товарных остатках.
        campaign_id (str): ID кампании в API и магазина в кабинете.
        access_token (str):  Авторизационный токен.

    Returns:
        dict: Обновленные остатки.

    Examples:
        >>> update_stocks(stocks, campaign_id, access_token)
        {"status": "OK", "result": {"skus": [{"sku": "12345", "count": 10}, {"sku": "67890", "count": 5}]}}

        >>> update_stocks(stocks, campaign_id, wrong_access_token)
        {"status": "ERROR", "errors": [{"code": "UNAUTHORIZED", "message": "Invalid access token"}]}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены на товары

    Args:
        prices (list): Данные о ценах на товары.
        campaign_id (str): ID кампании в API и магазина в кабинете.
        access_token (str):  Авторизационный токен.

    Returns:
        dict: Обновленные цены.

    Examples:
         >>> update_price(prices, campaign_id, access_token)
        {"status": "OK", "result": {"offers": [{"sku": "12345", "price": 1000}, {"sku": "67890", "price": 1500}]}}

        >>> update_price(prices, campaign_id, wrong_access_token)
        {"status": "ERROR", "errors": [{"code": "UNAUTHORIZED", "message": "Invalid access token"}]}
    """    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id (str): ID кампании в API и магазина в кабинете.
        market_token (str): Авторизационный токен.

    Returns:
        list: Идентификаторы товаров на Я.Маркете

    Examples:
        >>> get_offer_ids(campaign_id, market_token)
        ["12345", "67890", "11223"]

        >>> get_offer_ids(wrong_campaign_id, market_token)
        []
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список остатков товаров для загрузки на Яндекс Маркет.

    Args:
        watch_remnants (list): Товары с остатками из внешнего источника.
        offer_ids (list): Идентификаторы товаров на Я.Маркете.
        warehouse_id (str): Идентификатор склада, где хранятся товары.

    Returns:
        list: Список остатков товаров, готовых для загрузки на Яндекс Маркет.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        [{"sku": "12345", "warehouseId": "1", "items": [{"count": 100, "type": "FIT", "updatedAt": "2024-11-13T10:00:00Z"}]}]

        >>> create_stocks(watch_remnants, [], warehouse_id)
        [{"sku": "12345", "warehouseId": "1", "items": [{"count": 0, "type": "FIT", "updatedAt": "2024-11-13T10:00:00Z"}]}]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен для товаров на основе остатков.

    Args:
        watch_remnants (list): Товары с остатками из внешнего источника.
        offer_ids (list): Идентификаторы товаров на Я.Маркете.

    Returns:
        list: Цены для товаров, готовых для загрузки на Яндекс Маркет.

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        [{"id": "12345", "price": {"value": 1000, "currencyId": "RUR"}}]

        >>> create_prices(watch_remnants, wrong_offer_ids)
        {"status":"OK","errors":[{"code":"str","message":"str"}]}
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "str",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены товаров на Маркет

    Args:
        watch_remnants (list): Товары с остатками из внешнего источника.
        campaign_id (str): ID кампании в API и магазина в кабинете.
        market_token (str): Авторизационный токен.

    Returns:
        list: Список цен для товаров, которые были загружены на Яндекс Маркет.

    Examples:
        >>> await upload_prices(watch_remnants, campaign_id, market_token)
        [{"id": "12345", "price": {"value": 1000, "currencyId": "RUR"}}]

        >>> await upload_prices(watch_remnants, campaign_id, market_token)
        {"status":"OK","errors":[{"code":"str","message":"str"}]}
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остатки товаров на Яндекс Маркет.

    Args:
        watch_remnants (list): Товары с остатками из внешнего источника.
        campaign_id (str): ID кампании в API и магазина в кабинете.
        market_token (str): Авторизационный токен.
        warehouse_id (str): ID склада, на котором хранятся товары.

    Returns:
        list: Список товаров с остатками больше нуля.
        list: Список всех товаров с остатками.

    Examples:
        >>> await upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id)
        ([{"sku": "12345", "items": [{"count": 10}]}], [{"sku": "12345", "items": [{"count": 10}]}])

        >>> await upload_stocks(watch_remnants, campaign_id, market_token, wrong_warehouse_id)
        {"status":"OK","errors":[{"code":"str","message":"str"}]}
    """    
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
