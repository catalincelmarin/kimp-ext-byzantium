import importlib


class Helpers:
    @staticmethod
    def get_class(module_path):

        try:
            mod_path, class_name = module_path.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            imported = getattr(mod, class_name)

            return imported
        except Exception as e:
            raise e

    @staticmethod
    def get_method(instance, method_name):
        """
        Expects full path to class or module, plus method name.
        E.g., "some.module.MyClass", "my_method"
        """
        try:
            method = getattr(instance, method_name)

            return method
        except Exception as e:
            raise e
