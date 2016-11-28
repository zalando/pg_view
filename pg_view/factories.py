from pg_view.models.displayers import JsonDisplayer, ConsoleDisplayer, CursesDisplayer, OUTPUT_METHOD

OUTPUT_METHODS_TO_DISPLAYER = {
    OUTPUT_METHOD.console: ConsoleDisplayer,
    OUTPUT_METHOD.json: JsonDisplayer,
    OUTPUT_METHOD.curses: CursesDisplayer
}


def get_displayer_by_class(method, collector, show_units, ignore_autohide, notrim):
    if method not in OUTPUT_METHODS_TO_DISPLAYER:
        raise Exception('Output method {0} is not supported'.format(method))
    klass = OUTPUT_METHODS_TO_DISPLAYER[method]
    return klass.from_collector(collector, show_units, ignore_autohide, notrim)
