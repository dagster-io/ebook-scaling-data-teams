import dlt


@dlt.source
def my_source():
    @dlt.resource
    def s3():
        yield "hello, world!"

    return s3


my_load_source = my_source()
my_load_pipeline = dlt.pipeline()
