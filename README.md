# maillog parser
# тестовое задание

**$VERSION = 1.0**


Рабочая веб-страница с формой запроса данных доступна по адресу:
http://34369.ru/cgi-bin/request.pl


#### Содержимое
**Task.pdf** - текст задания
**parser.pl** - скрипт созданного парсера
**out** - полученный вместе с заданием файл лога для парсинга, при парсинге должен лежать в одной папке с парсером
**request.pl** - код html-страницы для выборки данных из таблиц базы
**gpbtest.sql** - дамп структцры созданной mysql-базы с таблицами (в задании пример для pgsql, у mysql синтакис отличается)


#### Использование  
Файл лога out положить в папку с файлом парсера parser.pl
Далее прямой запуск parser.pl для разбора файла лога.  
После отработки парсера (1-2 сек.) на веб-странице http://34369.ru/cgi-bin/request.pl можно запрашивать структурированные данные.  

#### Примеры для теста веб-страницы:
ldtyzggfqejxo@mail.ru найдет более 100 строк, выведет 100 и сообщит что есть еще строки
etrtemni@mail.ru найдет и выведет 29 строк


