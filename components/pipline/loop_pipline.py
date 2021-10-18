from .base_pipline import BasePipline


class LoopPipline(BasePipline):
    def __init__(self):
        super().__init__()
        self.component_param_tuple_list = []
        self.loop_judgement_func = None

    def append_component(self, component_class, init_params):
        self.component_param_tuple_list.append((component_class, init_params))
        return self

    def set_loop_judgement_func(self, func):
        self.loop_judgement_func = func
        return self

    def run(self, **kwargs):
        while self.loop_judgement_func(**kwargs):
            for component_class, init_params in self.component_param_tuple_list:
                component = self.init_component(component_class, init_params)
                kwargs = component.run(**kwargs)
        return kwargs
