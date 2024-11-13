import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон

    Args:
        last_id (str): Идентификатор последнего значения на странице.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict:  Словарь, содержащий список товаров, возвращаемый Ozon API.

    Examples:
        >>> get_product_list("12345", "client_id_example", "token_example")
        {'result': {'items': [{'product_id': 223681945, 'offer_id': '136748'}], 'total': 1, 'last_id': 'bnVсbA=='}}.

        >>> get_product_list("dfdsfwe", "client_id_example", "token_example")
        {'code': 0, 'details': [{'typeUrl': 'str', 'value': 'str'}], 'message': 'str'}.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        list: Список артикулов товаров

    Examples:
        >>> get_offer_ids("client_id_example", "token_example")
        ['136748', '136749']

        >>> get_offer_ids("incorrect_client_id", "token_example")
        {'code': 0, 'details': [{'typeUrl': 'str', 'value': 'str'}], 'message': 'Invalid client ID.'}
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Args:
        prices (list): Список цен, которые должны быть обновлены.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Ответ от API с информацией об обновленных ценах.

    Examples:
        >>> update_price([{"offer_id": "136748", "price": 5990}], "client_id_example", "token_example")
        {'result': {'updated_count': 1}, 'message': 'Prices updated successfully.'}

        >>> update_price([{"offer_id": "136748", "price": 5990}], "wrong_client_id", "wrong_token")
        {'code': 0, 'details': [{'typeUrl': 'str', 'value': 'str'}], 'message': 'Invalid API key.'}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров в магазине Озон.

    Эта функция отправляет запрос на обновление остатков товаров на платформе Озон.

    Args:
        stocks (list): Список словарей, каждый из которых содержит информацию о товаре
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ продавца.

    Returns:
        dict: Подтверждение успешного обновления остатков.

    Examples:
        >>> update_stocks([{'offer_id': '136748', 'stock': 100}], 'client_id_example', 'token_example')
        {'status': 'success', 'message': 'Остатки обновлены успешно'}

        >>> update_stocks([{'offer_id': '136748', 'stock': 0}], 'invalid_client_id', 'invalid_token')
        {'code': 0, 'details': [{'typeUrl': 'str', 'value': 'str'}], 'message': 'Invalid credentials'}
    """   
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатков с сайта Casio.

    Эта функция загружает файл остатков товаров с сайта Casio.

    Returns:
        list: Информацию об остатках часов, загруженную из файла Excel.

    Examples:
        >>> download_stock()
        [{'product_id': '12345', 'name': 'Casio Watch Model 1', 'stock': 20},
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список обновленных остатков товаров для загрузки в систему.

    Функция принимает список артикулов товаров, и создает новый список, 
    который можно загрузить на платформу.

    Args:
        watch_remnants (list): Список остатков товаров, загруженных с сайта.
        offer_ids (list): Список артикулов товаров, которые должны быть обновлены в системе.

    Returns:
        list: Список словарей с артикулом товара и его количеством (stock), готовый для загрузки в систему.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids)
        [{'offer_id': '12345', 'stock': 5}, {'offer_id': '67890', 'stock': 100}, {'offer_id': '11111', 'stock': 0}]

        >>> watch_remnants = [{'Код': '12345', 'Количество': '5'}, {'Код': '67890', 'Количество': '>10'}]
        >>> offer_ids = ['12345', '67890', '11111']
        >>> create_stocks(watch_remnants, offer_ids)
        TypeError: '>' not supported between instances of 'str' and 'int'
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен для товаров.

    Принимает список остатков и артикулов, фильтрует товары по артикулу и 
    формирует список с ценами для каждого товара.

    Args:
        watch_remnants (list): Список остатков с ценами.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список словарей, содержащих информацию о ценах для товаров.

    Examples:
        >>> watch_remnants = [{'Код': '12345', 'Цена': '1000'}, {'Код': '67890', 'Цена': '2000'}]
        >>> offer_ids = ['12345', '67890', '11111']
        >>> create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '12345', 'old_price': '0', 'price': '1000'},
         {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '67890', 'old_price': '0', 'price': '2000'}]

        >>> watch_remnants = [{'Код': '12345', 'Цена': '1000'}, {'Код': '67890', 'Цена': '500'}]
        >>> offer_ids = ['11111']
        >>> create_prices(watch_remnants, offer_ids)
        []
        """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену в числовой формат без разделителей и валюты.

    Удаляет все нечисловые символы и оставляет толкьо целую часть стоимости.
    Преобразует строку вида '5'990.00 руб.' в формат '5990'.

    Args:
        price (str): Стоимость, содержащая валюту и разделители.

    Returns:
        str: Стоимость без разделителей и валюты.

    Examples:
        >>> price_conversion("3 490 руб.")
        '3490'

        >>> price_conversion("Неизвестно")
        ''  # Возвращает пустую строку, так как нет числовых символов.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список на части по n элементов.

    Разделяет список на несколько частей, каждая часть содержит не более `n`
    элементов.

    Args:
        lst (list): Список, который нужно разделить.
        n (int): Число элементов в каждой части.

    Yields:
        list: Подсписки длиной не более `n` элементов.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7, 8], 3))
        [[1, 2, 3], [4, 5, 6], [7, 8]]

        >>> list(divide(1, 2))
        Error
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить обновленные цены для товаров на Озон.

    Функция получает список остатков товаров и обновляет цены

    Args:
        watch_remnants (list): Список остатков товаров с ценами.
        client_id (str): Идентификатор клиента для API Озон.
        seller_token (str): API-ключ продавца.

    Returns:
        list: Список цен, который был обновлен и отправлен в систему.

    Examples:
        >>> await upload_prices(watch_remnants_data, "client_id_example", "token_example")
        [{'offer_id': '136748', 'price': '500'}, {'offer_id': '136749', 'price': '1000'}, ...]

        >>> await upload_prices(watch_remnants_data, "client_id_example", "token_wrong")
        Error
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить обновленные остатки товаров на Озон.

    Функция получает список остатков товаров и обновляет их на платформе Озон.

    Args:
        watch_remnants (list): Список остатков товаров.
        client_id (str): Идентификатор клиента для API Озон.
        seller_token (str): API-ключ продавца для доступа к данным.

    Returns:
        not_empty: Список товаров с остатками больше нуля.
        stocks: Полный список всех товаров с их остатками.

    Examples:
        >>> await upload_stocks(watch_remnants_data, "client_id_example", "token_example")
        ([{'offer_id': '136748', 'stock': 50}, {'offer_id': '136749', 'stock': 30}], [{'offer_id': '136748', 'stock': 50}, {'offer_id': '136749', 'stock': 30}, {'offer_id': '136750', 'stock': 0}])

        >>> await upload_stocks([], "", "")
        ([], [])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
