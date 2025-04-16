from neo4j import GraphDatabase
import uuid

class WeightedTrie:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._initialize_db()

    def close(self):
        self._driver.close()

    def _initialize_db(self):
        with self._driver.session() as session:
            session.execute_write(self._create_root_if_not_exists)

    @staticmethod
    def _create_root_if_not_exists(tx):
        tx.run("MERGE (root:Root {id: 'root'})")

    def add_word(self, word, weight=1):
        with self._driver.session() as session:
            session.execute_write(self._add_word_transaction, word.lower(), weight)

    @staticmethod
    def _add_word_transaction(tx, word, weight):
        current_node = "root"
        
        for i, char in enumerate(word):
            next_node = f"{current_node}_{char}"
            is_end = i == len(word) - 1
            
            tx.run("""
                MATCH (current {id: $current_id})
                MERGE (current)-[r:HAS_CHAR {weight: $weight}]->(next:Node {
                    id: $next_id, 
                    char: $char,
                    is_end: $is_end
                })
                ON CREATE SET next.word = CASE WHEN $is_end THEN $word ELSE null END
                RETURN r
            """, current_id=current_node, next_id=next_node, char=char, 
                 weight=weight, is_end=is_end, word=word)
            
            current_node = next_node

    def search_word(self, word):
        with self._driver.session() as session:
            return session.execute_read(self._search_word_transaction, word.lower())

    @staticmethod
    def _search_word_transaction(tx, word):
        # Преобразуем слово в список символов для сравнения
        chars = list(word)
        result = tx.run("""
            MATCH path = (root:Root {id: 'root'})-[:HAS_CHAR*]->(end)
            WHERE ALL(i IN range(0, size(relationships(path))-1) WHERE 
                  (nodes(path)[i+1]).char = $chars[i])
            AND end.is_end = true
            AND end.word = $word
            RETURN end, reduce(total = 0, r in relationships(path) | total + r.weight) AS totalWeight
        """, chars=chars, word=word)
        
        return [{"node": record["end"], "total_weight": record["totalWeight"]} for record in result]

    def starts_with(self, prefix):
        with self._driver.session() as session:
            return session.execute_read(self._starts_with_transaction, prefix.lower())

    @staticmethod
    def _starts_with_transaction(tx, prefix):
        chars = list(prefix)
        result = tx.run("""
            MATCH path = (root:Root {id: 'root'})-[:HAS_CHAR*]->(end)
            WHERE ALL(i IN range(0, size(relationships(path))-1) WHERE 
                  (nodes(path)[i+1]).char = $chars[i])
            RETURN end, reduce(total = 0, r in relationships(path) | total + r.weight) AS totalWeight
            ORDER BY totalWeight DESC
        """, chars=chars)
        
        return [{"node": record["end"], "total_weight": record["totalWeight"]} for record in result]

    def delete_word(self, word):
        with self._driver.session() as session:
            session.execute_write(self._delete_word_transaction, word.lower())

    @staticmethod
    def _delete_word_transaction(tx, word):
        tx.run("""
            MATCH path = (root:Root {id: 'root'})-[:HAS_CHAR*]->(end)
            WHERE end.is_end = true AND end.word = $word
            SET end.is_end = false
            REMOVE end.word
        """, word=word)

    def get_all_words(self):
        with self._driver.session() as session:
            return session.execute_read(self._get_all_words_transaction)

    @staticmethod
    def _get_all_words_transaction(tx):
        result = tx.run("""
            MATCH path = (root:Root {id: 'root'})-[:HAS_CHAR*]->(end)
            WHERE end.is_end = true
            RETURN end.word AS word, 
                   reduce(total = 0, r in relationships(path) | total + r.weight) AS totalWeight
            ORDER BY totalWeight DESC
        """)
        
        return [{"word": record["word"], "weight": record["totalWeight"]} for record in result]


# Пример использования
if __name__ == "__main__":
    # Подключение к Neo4j (замените параметры на свои)
    trie = WeightedTrie("bolt://localhost:7687", "neo4j", "password")

    try:
        # Добавление слов с весами
        trie.add_word("apple", 5)
        trie.add_word("app", 3)
        trie.add_word("application", 7)
        trie.add_word("banana", 4)
        trie.add_word("band", 2)

        # Поиск слова
        print("Поиск 'apple':", trie.search_word("apple"))
        print("Поиск 'app':", trie.search_word("app"))
        print("Поиск 'nonexistent':", trie.search_word("nonexistent"))

        # Поиск по префиксу
        print("Слова, начинающиеся на 'app':", trie.starts_with("app"))
        print("Слова, начинающиеся на 'ban':", trie.starts_with("ban"))

        # Получение всех слов
        print("Все слова в дереве:", trie.get_all_words())

        # Удаление слова
        trie.delete_word("app")
        print("После удаления 'app':", trie.search_word("app"))

    finally:
        trie.close()