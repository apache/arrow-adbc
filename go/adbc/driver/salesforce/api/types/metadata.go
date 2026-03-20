package types

type MetadataRequest struct {
	Dataspace      string
	EntityCategory string
	EntityName     string
	EntityType     string
}

type MetadataResponse struct {
	Metadata []MetadataEntity `json:"metadata"`
}

type MetadataEntity struct {
	Name          string                `json:"name"`
	DisplayName   string                `json:"displayName,omitempty"`
	Label         string                `json:"label"`
	Category      string                `json:"category"`
	EntityType    string                `json:"entityType"`
	Fields        []MetadataField       `json:"fields"`
	Dimensions    []MetadataDimension   `json:"dimensions,omitempty"`
	Measures      []MetadataMeasure     `json:"measures,omitempty"`
	Relationships []MetadataRelationship `json:"relationships,omitempty"`
}

type MetadataField struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	Label       string `json:"label"`
	Type        string `json:"type"`
	Nullable    bool   `json:"nullable"`
	PrimaryKey  bool   `json:"primaryKey"`
}

type MetadataDimension struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	Type        string `json:"type"`
}

type MetadataMeasure struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	Type        string `json:"type"`
}

type MetadataRelationship struct {
	FromEntity          string `json:"fromEntity"`
	FromEntityAttribute string `json:"fromEntityAttribute"`
	ToEntity            string `json:"toEntity"`
	ToEntityAttribute   string `json:"toEntityAttribute"`
	Cardinality         string `json:"cardinality"`
}
