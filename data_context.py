from collections.abc import Iterable


"""
グローバルでデータ管理することを想定としたデータ管理クラス

使用用途
- 再利用率の高いデータをコンテキスト管理したいとき
- DBから毎回データを取得したくないとき

サンプルコード

```py
ctx = DataContext()
def main():
    # DataContextクラスをグローバルでインスタンス化した場合は始めに動的確保データをリセットすることを推奨
    ctx.deinit()
    
    # RDBからデータを取得することを想定しているので
    # List[Dict[...]] や Dict[...] のような型となる
    data_A = [{"column_A": "hoge", ...},{...}] # もしくは {"column_A": "hoge", ...}
    
    # コンテキストにセット
    ctx.set("data_A", data_A)

    # find
    column = ("column_A",)
    value = ("hoge",)
    record = ctx.find(ctx.get("data_A"), column, value)
    print(record) # {"column_A": "hoge", ...}

    # filter
    column = ("column_A",)
    value = ("hoge",)
    records = ctx.filter(
        ctx.get("data_A", []),
        column,
        value
    )
    print(records) # [{"column_A": "hoge", ...},{...}]
```
"""
class DataContext:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataContext, cls).__new__(cls)
            cls._instance._data = {}
        return cls._instance

    def set(self, key: str, value):
        self._data[key] = value

    def get(self, key: str, default=None):
        return self._data.get(key, default)
    
    def deinit(self):
        self._data.clear()

    @staticmethod
    def _match(val, cond) -> bool:
        """ 単一値は比較、複数値(condがリストなど)は in 判定 """
        if isinstance(cond, Iterable) and not isinstance(cond, (str, bytes, dict)):
            return val in cond
        return val == cond

    def find(
        self,
        records: list[dict],
        keys: str | Iterable[str],
        values,
        default=None
    ) -> dict | None:
        """
        条件を満たす最初のレコードを返す
        条件を満たすレコードがない場合は default を返す
        """
        # キー列・値列を揃える
        if isinstance(keys, Iterable) and not isinstance(keys, (str, bytes, dict)):
            key_seq = list(keys)
            if not (isinstance(values, Iterable) and not isinstance(values, (str, bytes, dict))):
                raise ValueError("複数キーの場合、values も Iterable である必要があります")
            value_seq = list(values)
            if len(key_seq) != len(value_seq):
                raise ValueError("keys と values の長さが一致しません")
        else:
            key_seq   = [keys]            # 単一キー
            value_seq = [values]          # 単一値 or メンバーシップ判定用 Iterable

        return next(
            (
                r for r in records
                if all(self._match(r.get(k), v) for k, v in zip(key_seq, value_seq))
            ),
            default
        )

    def filter(
        self,
        records: list[dict],
        keys: str | Iterable[str],
        values
    ) -> list[dict]:
        """ 条件を満たすすべてのレコードを返す """
        # キー列・値列を揃える（find_record と同じ処理）
        if isinstance(keys, Iterable) and not isinstance(keys, (str, bytes, dict)):
            key_seq = list(keys)
            if not (isinstance(values, Iterable) and not isinstance(values, (str, bytes, dict))):
                raise ValueError("複数キーの場合、values も Iterable である必要があります")
            value_seq = list(values)
            if len(key_seq) != len(value_seq):
                raise ValueError("keys と values の長さが一致しません")
        else:
            key_seq   = [keys]
            value_seq = [values]

        return [
            r for r in records
            if all(self._match(r.get(k), v) for k, v in zip(key_seq, value_seq))
        ]
