from .base_pipline import BasePipline


class SwitchPipline(BasePipline):
    def __init__(self):
        super().__init__()
        self.branch_component_param_tuple_dict = {}
        self.switch_judgement_func = None

    def set_branch(self, branch_name, component_class, init_params):
        self.branch_component_param_tuple_dict[branch_name] = (component_class, init_params)
        return self

    def set_switch_judgement_func(self, func):
        self.switch_judgement_func = func
        return self

    def run(self, **kwargs):
        branch_name = self.switch_judgement_func(**kwargs)
        component_class, init_params = self.branch_component_param_tuple_dict[branch_name]
        component = self.init_component(component_class, init_params)
        return component.run(**kwargs)
