# Concept

In dialog **concept**s provide a way to model data in terms of relations. We often use YAML to describe concepts here is an example:

```yaml
# each operator can define concept under their own namespace in
# an abstract way
diy.cook:
  # Atomic concept that is of type string.
  Text:
    this:
      description: Some text
      type: string

  # Email is a string but has a different semantic meaning.
  # You can think of as struct Email(string) in rust.
  Email:
    this:
      description: Some email
      type: string

  # Describes concept of the recipe tool is working with in terms
  # of an entity and set of relations related to it.
  Recipe:
    the: entity
    :: A meal recipe
    # The recipe concept has notion of a title.
    title:
      # Titile is expected to be a Text, which is
      # built-in concept and is effectively a string.
      type: text
      description: The name of the recipie

      # The recipe has related ingredients, unlike title it may
      #  have multiple ingredients associated with it. Concept
      # does not define any constraint on what related value
      # should be, usually this implies that it is another
      # entity.
      ingredient:
        describe: Ingredients of the recipe
        type: Ingredient
        cardinality: many

      # The recipe has an associated steps.
      steps:
        # Steps are considered associated if they meet definition of the RecipeStep concept
        type: RecipeStep
        describe: Steps of the cooking process
        cardinality: many



diy.cook.rules:
  as-text:
    assert:
      Text: ?content
    when:
      - content/type:
        of: ?content
        is: string

  as-email:
    assert:
      Email: ?content
    when:
      - Data/Type:
        of: ?content
        is: string
      - Pattern/Match:
        pattern: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$
        content: ?content




  # Describes concept of the recipe tool is working with in terms
  # of an entity and set of relations related to it.
  Recipe:
    # By convention `this` represents an entity. It is implied
    # when omitted however it is recommended to use it in order
    # to provide description.
    this:
      description: Meal recipie
    # The recipe concept has notion of a title.
    title:
      description: The name of the recipie
      # Titile is expected to be a Text, which is
      # built-in concept and is effectively a string.
      as: dialog.Text
      # The recipe has related ingredients, unlike title it may have
          # multiple ingredients associated with it. Concept does not define
          # any constraint on what related value should be, usually this
          # implies that it is another entity.
          ingredient:
            the: Ingredients of the recipe
            cardinality: many
```
