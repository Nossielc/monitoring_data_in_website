import scrapy
import pyodbc
import pandas as pd
import smtplib 

from email.message import EmailMessage
from scrapy import Spider
from scrapy.http import Request

class MonitoringWege3Spider(Spider):
    name = 'monitoring_wege3'
    allowed_domains = ['www.guiainvest.com.br']
    start_urls = ['https://www.guiainvest.com.br/raiox/default.aspx']
     
    def __init__(self):
      self.sql_conn = self.mssqlConnect()

    def start_requests(self):
      queryAcoes = "SELECT [SIGLA] FROM [dbo].[SLN_ATIVO] WHERE ATIVO = 'S' AND TIPO_ACAO != 'FII' \
								                                                            AND TIPO_ACAO != 'DRN' \
                                                                            AND TIPO_ACAO != 'DR2' \
                                                                            AND TIPO_ACAO != 'DR3' \
								                                                            AND TIPO_ACAO != 'CI' \
                                                                            AND TIPO_ACAO != 'TPR' order by CODIGO DESC"
      dfAcoes = pd.read_sql(queryAcoes, self.sql_conn)
      for index,row in dfAcoes.iterrows():
        yield scrapy.Request("https://www.guiainvest.com.br/raiox/default.aspx?sigla=" + row['SIGLA'], self.parse)

    def parse(self, response):
      stock = response.url.split('=')[1]
      lpa_trimestre_atual = response.xpath('//div[@id="areaConteudoInner"]/div[@class="innerContainer"]/div[@class="innerContainerFeed"]/div[@id="divIndicadores"]/table/tr[10]/td[4]/span/text()').extract_first()
      if(lpa_trimestre_atual == '-' or lpa_trimestre_atual is None):
        if( self.lpaLastTrimIsNull(stock) ):
          self.sendEmailNotificationOnMissingData(stock)

    def sendEmailNotificationOnMissingData(self, stock):
      EmailAdd = "monitor.raiox@gmail.com"
      Pass = "guiainvest"

      msg = EmailMessage()
      msg['Subject'] = 'Raio X missing data to ' + str(stock)
      msg['From'] = EmailAdd
      msg['To'] = 'cleisson.flores@guiainvest.com.br'
      msg.set_content('Falha ao recuperar os dados de LPA do trimestre atual no raiox X do ativo ' + str(stock)) # Email body or Content

      with smtplib.SMTP_SSL('smtp.gmail.com',465) as smtp:
        smtp.login(EmailAdd,Pass)
        smtp.send_message(msg)

    def mssqlConnect(self,env = "debug"):
      host = "YourDevEnviromentHost"
      if (env == "PRD"):
        host = "YourProductionHost"
      host
      return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                               SERVER=%s; \
                               DATABASE=YourDatabase; \
                               UID=YourUser; \
                               PWD=YourPassword' % (host))

    def lpaLastTrimIsNull(self, stock):
      queryLpa = "SELECT \
	                    dfp.INFI_LUCRO_LIQUIDO_ACAO_12M as lpa \
                    FROM SLN_ATIVO AS ativ \
                    INNER JOIN [SLN_CACHE_DFP_ATIVO_API] as dfp \
	                    ON ativ.CODIGO = dfp.SLN_ATIV_CODIGO \
                    WHERE ativ.SIGLA = '%s' \
	                      and ativ.ATIVO = 'S' \
	                      and dfp.DATA_REFERENCIA = ( select max(DATA_REFERENCIA) \
									                                  from [SLN_CACHE_DFP_ATIVO_API] \
									                                  where ativ.SIGLA = '%s' \
								                                  ) \
                      order by DATA_REFERENCIA DESC" % (stock,stock)
      dfLpa = pd.read_sql(queryLpa, self.sql_conn)
      if ( len(dfLpa.index) == 0 ):
        return 0
      elif ( len(dfLpa.index) == 1 ):
        if(dfLpa['lpa'][0] is None):
          return 0
      elif ( len(dfLpa.index) == 2 ):
        if(dfLpa['lpa'][0] is None and dfLpa['lpa'][1] is None):
          return 0
      return 1