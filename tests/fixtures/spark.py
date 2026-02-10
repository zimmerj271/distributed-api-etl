class FakeSparkConf:
    def get(self, key: str) -> str:
        if key == "spark.driver.host":
            return "localhost"
        raise KeyError(key)


class FakeSparkContext:
    def getConf(self) -> FakeSparkConf:
        return FakeSparkConf()


class FakeSparkSession:
    sparkContext = FakeSparkContext()

