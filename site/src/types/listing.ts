// Canonical listing types. Mirrors docs/SCHEMA.md exactly.

export type Category =
  | "veterinarian"
  | "groomer"
  | "boarder"
  | "daycare"
  | "sitter"
  | "shelter"
  | "pet_hotel"
  | "dog_park"
  | "pet_cafe"
  | "pet_memorial";

export type Metro =
  | "atlanta"
  | "tampa"
  | "austin"
  | "nashville"
  | "asheville";

export type Source =
  | "osm"
  | "napps"
  | "psi"
  | "ibpsa"
  | "ndgaa"
  | "claim";

export interface Listing {
  // Identity
  id: string;
  name: string;
  category: Category;
  subcategories?: string[];

  // Location
  address: string;
  city: string;
  state: string;
  zip: string;
  metro: Metro;
  lat: number;
  lng: number;

  // Contact
  phone?: string;
  website?: string;
  email?: string;

  // Provenance
  sources: Source[];
  sourceIds: Record<string, string>;
  lastSeenAt: string;

  // Enrichment
  hours?: Record<string, string>;
  description?: string;
  tags?: string[];
  attributes?: string[]; // computed flags, e.g. "emergency", "exotic", "cat_friendly"

  // Directory state
  claimed: boolean;
  claimedAt?: string;
}

export const METROS: Metro[] = [
  "atlanta",
  "tampa",
  "austin",
  "nashville",
  "asheville",
];

export const CATEGORIES: Category[] = [
  "veterinarian",
  "groomer",
  "boarder",
  "daycare",
  "sitter",
  "shelter",
  "pet_hotel",
  "dog_park",
  "pet_cafe",
  "pet_memorial",
];

export const METRO_LABELS: Record<Metro, string> = {
  atlanta: "Atlanta",
  tampa: "Tampa",
  austin: "Austin",
  nashville: "Nashville",
  asheville: "Asheville",
};

export const METRO_STATES: Record<Metro, string> = {
  atlanta: "GA",
  tampa: "FL",
  austin: "TX",
  nashville: "TN",
  asheville: "NC",
};

export const CATEGORY_LABELS: Record<Category, string> = {
  veterinarian: "Veterinarians",
  groomer: "Groomers",
  boarder: "Boarders",
  daycare: "Daycares",
  sitter: "Sitters",
  shelter: "Shelters",
  pet_hotel: "Pet hotels",
  dog_park: "Dog parks",
  pet_cafe: "Pet cafes",
  pet_memorial: "Pet memorials",
};

export const CATEGORY_LABELS_SINGULAR: Record<Category, string> = {
  veterinarian: "Veterinarian",
  groomer: "Groomer",
  boarder: "Boarder",
  daycare: "Daycare",
  sitter: "Sitter",
  shelter: "Shelter",
  pet_hotel: "Pet hotel",
  dog_park: "Dog park",
  pet_cafe: "Pet cafe",
  pet_memorial: "Pet memorial",
};
