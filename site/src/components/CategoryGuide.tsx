import type { Category } from "@/types/listing";

const GUIDES: Record<
  Category,
  { title: string; points: string[] }
> = {
  veterinarian: {
    title: "What to look for in a vet",
    points: [
      "Ask about emergency and after-hours availability — not every clinic offers it.",
      "Check whether they see exotic or specialty pets if you need that.",
      "Keep vaccine and visit records handy — FetchFiles makes it one tap to share.",
    ],
  },
  groomer: {
    title: "Picking a groomer",
    points: [
      "Call ahead to confirm breed experience — small-dog groomers don't always do doubles coats.",
      "Ask about vaccine requirements; most require up-to-date rabies and Bordetella.",
      "A quick drop-off visit before a full appointment helps skittish pets acclimate.",
    ],
  },
  boarder: {
    title: "What boarders will ask for",
    points: [
      "Most require proof of rabies, DHPP, and Bordetella — FetchFiles can share these instantly.",
      "Confirm feeding schedule, medications, and emergency contact details in writing.",
      "Ask whether overnight staff are on-site or on-call.",
    ],
  },
  daycare: {
    title: "Before your first daycare visit",
    points: [
      "Most daycares run a temperament assessment before accepting new pets.",
      "Bring current vaccination records (rabies, DHPP, Bordetella, often Flu H3N2/H3N8).",
      "Ask about the dog-to-staff ratio and separation of size/play groups.",
    ],
  },
  sitter: {
    title: "Questions to ask a sitter",
    points: [
      "Confirm they carry pet-sitter insurance and are bonded.",
      "Ask about medication administration, and what happens in an emergency.",
      "Share FetchFiles records ahead of time so they have vet contact and meds on hand.",
    ],
  },
  shelter: {
    title: "If you're adopting from this shelter",
    points: [
      "Ask about the pet's medical history — most shelters provide it on request.",
      "Many shelters include initial vaccinations and spay/neuter in the adoption fee.",
      "Start a FetchFiles record on day one so new vet visits and meds are already organized.",
    ],
  },
  pet_hotel: {
    title: "Before your pet's stay",
    points: [
      "Pet hotels are premium boarding — ask about staff-to-pet ratios overnight.",
      "Current rabies, DHPP, and Bordetella are universally required; share via FetchFiles.",
      "Check on enrichment (play groups, walks, webcams) — not all hotels include the same amenities.",
    ],
  },
  dog_park: {
    title: "Bringing your dog here",
    points: [
      "Most off-leash parks require current rabies and DHPP vaccines.",
      "Check if there's a size-separated area — small dogs can be overwhelmed in general enclosures.",
      "Bring water and waste bags; some parks stock them, many don't.",
    ],
  },
  pet_cafe: {
    title: "What to know",
    points: [
      "Treats and baked goods here are made for pets — ingredients are safe, but moderation matters.",
      "Ask about ingredient lists if your pet has allergies or sensitivities.",
      "Many pet bakeries host birthday or adoption-day cakes — call ahead for custom orders.",
    ],
  },
  pet_memorial: {
    title: "When you need to say goodbye",
    points: [
      "Most memorial providers handle both burial and cremation — ask what's included.",
      "Private (individual) cremation returns only your pet's ashes; communal does not.",
      "Many vets coordinate directly with the memorial provider — your clinic may already have a preferred partner.",
    ],
  },
};

export function CategoryGuide({ category }: { category: Category }) {
  const g = GUIDES[category];
  return (
    <div className="rounded-xl border border-warm-100 bg-warm-50 p-5">
      <h2 className="font-semibold text-ink-900 mb-3">{g.title}</h2>
      <ul className="space-y-2 text-sm text-ink-700">
        {g.points.map((p, i) => (
          <li key={i} className="flex gap-2">
            <span className="text-warm-600 font-semibold">•</span>
            <span>{p}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}
