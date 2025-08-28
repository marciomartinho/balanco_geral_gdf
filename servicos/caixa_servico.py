"""
Serviço de Caixa e Equivalente de Caixa
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import Dict, List, Any
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

    def _execute_query(self, query: str, params: Dict = None) -> List[Dict[str, Any]]:
        """Executa uma query e retorna uma lista de dicionários."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(query, params or {})
                colunas = [col[0].upper() for col in cursor.description]
                resultados = [dict(zip(colunas, row)) for row in cursor.fetchall()]
                return resultados
        except Exception as e:
            logger.error(f"Erro ao executar a query: {e}")
            return []

    def buscar_anos_disponiveis(self) -> List[int]:
        """Busca os anos (COEXERCICIO) distintos e disponíveis."""
        query = "SELECT DISTINCT COEXERCICIO FROM MIL2001.SALDOCONTABIL_EX ORDER BY COEXERCICIO DESC"
        resultados = self._execute_query(query)
        return [row['COEXERCICIO'] for row in resultados]

    def buscar_ugs_por_ano(self, ano: int) -> List[Dict[str, Any]]:
        """Busca as UGs (COUG, NOUG) distintas para um determinado ano."""
        query = """
            SELECT DISTINCT COUG, NOUG
            FROM MIL2001.SALDOCONTABIL_EX
            WHERE COEXERCICIO = :ano AND NOUG IS NOT NULL
            ORDER BY NOUG ASC
        """
        return self._execute_query(query, {'ano': ano})

    def buscar_resumo_por_ug(self, ano: int, ug: int) -> Dict[str, Any]:
        """Busca o resumo de caixa agrupado por conta corrente para uma UG."""
        logger.info(f"Buscando resumo para UG {ug} no ano {ano}")
        query = self.sql.carregar_query('caixa_resumo_ug') # Usa o novo arquivo SQL

        try:
            resultados = self._execute_query(query, {'ano': ano, 'ug': ug})

            dados_processados = []
            total_debito = 0
            total_credito = 0

            for row in resultados:
                debito = row.get('TOTAL_DEBITO', 0) or 0
                credito = row.get('TOTAL_CREDITO', 0) or 0
                saldo = debito - credito
                row['SALDO'] = saldo
                row['DC'] = 'D' if saldo >= 0 else 'C'
                dados_processados.append(row)
                
                total_debito += debito
                total_credito += credito
            
            saldo_geral = total_debito - total_credito
            totais = {
                'TOTAL_DEBITO': total_debito,
                'TOTAL_CREDITO': total_credito,
                'SALDO': saldo_geral,
                'DC': 'D' if saldo_geral >= 0 else 'C'
            }

            return {
                'sucesso': True,
                'dados': dados_processados,
                'totais': totais
            }

        except Exception as e:
            logger.error(f"Erro no serviço buscar_resumo_por_ug: {e}")
            return {'sucesso': False, 'erro': str(e)}