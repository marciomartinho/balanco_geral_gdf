"""
Serviço de Caixa e Equivalente de Caixa
Usa APENAS queries dos arquivos SQL
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import Dict, List, Optional, Any
import logging
from dados.conexao import get_db_manager
from dados.gerenciador_sql import get_gerenciador_sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CaixaServico:
    """Serviço para gerenciar dados de Caixa e Equivalente"""
    
    def __init__(self):
        self.db = get_db_manager()
        self.sql = get_gerenciador_sql()
        logger.info("Serviço de Caixa inicializado")
    
    def buscar_detalhes_caixa(
        self,
        ano: Optional[int] = None,
        mes: Optional[int] = None,
        limite: int = 100
    ) -> Dict[str, Any]:
        """Busca detalhes usando arquivo SQL"""
        ano = ano or datetime.now().year
        mes = mes or datetime.now().month
        
        logger.info(f"Buscando detalhes: {ano} até mês {mes}")
        
        # Carregar query do arquivo SQL
        query = self.sql.carregar_query('caixa_equivalente_detalhes')
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(query, {
                    'ano': ano,
                    'mes_limite': mes,
                    'limite': limite
                })
                colunas = [col[0] for col in cursor.description]
                resultados = cursor.fetchall()
            
            # Processar e calcular saldo
            dados_processados = []
            for row in resultados:
                registro = dict(zip(colunas, row))
                
                # Calcular saldo
                vadebito = float(registro.get('VADEBITO', 0) or 0)
                vacredito = float(registro.get('VACREDITO', 0) or 0)
                registro['SALDO'] = vadebito - vacredito
                
                dados_processados.append(registro)
            
            return {
                'sucesso': True,
                'dados': dados_processados,
                'total': len(dados_processados)
            }
            
        except Exception as e:
            logger.error(f"Erro: {e}")
            return {
                'sucesso': False,
                'erro': str(e),
                'dados': []
            }


if __name__ == "__main__":
    servico = CaixaServico()
    resultado = servico.buscar_detalhes_caixa(limite=5)
    if resultado['sucesso']:
        print(f"Total: {resultado['total']} registros")