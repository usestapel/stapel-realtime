"""A routing manifest that fails to import — must never be swallowed."""
raise RuntimeError("this module's routing is broken")
