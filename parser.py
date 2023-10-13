import requests
import json
import asyncio

from bs4 import BeautifulSoup as BS

import aiohttp 

from tqdm import tqdm

page_parse = "https://online.metro-cc.ru" 

def set_cookie(place:str):
    """
    Ставит куки для Питера и для Москвы
    """
    PLACES = {"MSK":10,
              "SPB":16,
             }
    place_searching = PLACES[place]
    cookies = {'metroStoreId':place_searching}
    return cookies

def pages_counter(place:str)->int:
    """
    Неасинхронно узнает сколько страничек нужно парсить
    """
    request = requests.get(f'{page_parse}/category/sladosti-chipsy-sneki/konfety-podarochnye-nabory?from=under_search&in_stock=1&page=1')
    raw_soup = BS(request.text,'html.parser')
    tag = raw_soup.find('ul',attrs= {"class":"catalog-paginate v-pagination"}).findAll("a")[-2]
    return int(tag.text)

async def parser(page:int,place:str)->list[dict]:
    """
    Ассинхронно запрашивает необходимую информацию и ввыводит list данных со странички
    """
    async with aiohttp.ClientSession(cookies=set_cookie(place)) as session:
        async with session.get(f'{page_parse}/category/sladosti-chipsy-sneki/konfety-podarochnye-nabory?from=under_search&in_stock=1&page={page}') as res:
            raw_soup = BS(await res.text(),'html.parser')
            raw_products_list = raw_soup.find(attrs={'id':'products-inner'}).findAll('div',attrs={'class':'catalog-2-level-product-card product-card subcategory-or-type__products-item catalog--common offline-prices-sorting--best-level with-prices-drop'})

            format_data_list = []

            for product in (pbar := tqdm(raw_products_list)):
                data = {}
                data["id"] = product["data-sku"]
                data["place"] = place
                photo_link = product.find("a")

                data["name"] = photo_link["title"]
                data["url"] = page_parse + photo_link["href"]
                pbar.set_description(f"Processing page №{page} {place}")
                
                price_old = product.find('span',attrs={"class":"product-price nowrap product-card-prices__old style--catalog-2-level-product-card-major-old catalog--common offline-prices-sorting--best-level"})
                if price_old:
                    price_old = price_old.find("span",attrs={"class":"product-price__sum-rubles"}).text

                price_actual = product.find('span',attrs={"class":"product-price nowrap product-card-prices__actual style--catalog-2-level-product-card-major-actual color--red catalog--common offline-prices-sorting--best-level"})
                if price_actual:
                    price_actual = price_actual.find("span",attrs={"class":"product-price__sum-rubles"}).text
    
                regular_price = product.find('span',attrs={"class":"product-price nowrap product-card-prices__actual style--catalog-2-level-product-card-major-actual catalog--common offline-prices-sorting--best-level"})
                if regular_price:
                    regular_price = regular_price.find("span",attrs={"class":"product-price__sum-rubles"}).text

                if regular_price:    
                    data["regular_price"] = regular_price
                else:
                    data["regular_price"] = price_old
                    data["promo_price"] = price_actual

                async with session.get(data['url']) as res:
                    raw_soup = BS(await res.text(),"html.parser")
                    brend = raw_soup.find("a",attrs={"class":"product-attributes__list-item-link reset-link active-blue-text"})
                    if brend:
                        data["brend"] = brend.text.strip()

                format_data_list.append(data) 
    return format_data_list

async def parse_all_MSK_SPB()->list[dict]:
    """
    Создает процессы загрузки информации со страничек
    """
    full_data = []
    len_page = pages_counter("SPB")
    pages_SPB = [parser(i,"SPB") for i in range(1,len_page+1)] 

    for page in (pbar:=tqdm(asyncio.as_completed(pages_SPB),total=len_page)):
        pbar.set_description("MAIN PROCESS SPB")
        for product in await page:
            full_data.append(product)

    len_page = pages_counter("MSK")
    pages_MSK = [parser(i,"MSK") for i in range(1,len_page+1)] 

    for page in (pbar:=tqdm(asyncio.as_completed(pages_MSK),total=len_page)):
        pbar.set_description("MAIN PROCESS MSK")
        for product in await page:
            full_data.append(product)

    return full_data

#как получим инфу - задампим в джонску
data = asyncio.run(parse_all_MSK_SPB())
with open("result.json","w") as json_file:
    json.dump(data,json_file, indent=3)



        
         






