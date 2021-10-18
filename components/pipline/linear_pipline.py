from .base_pipline import BasePipline


class LinearPipline(BasePipline):
    def __init__(self):
        super().__init__()
        self.component_param_tuple_list = []

    def append_component(self, component_class, init_params):
        self.component_param_tuple_list.append((component_class, init_params))
        return self

    def run(self, **kwargs):
        for component_class, init_params in self.component_param_tuple_list:
            component = self.init_component(component_class, init_params)
            kwargs = component.run(**kwargs)
        return kwargs
