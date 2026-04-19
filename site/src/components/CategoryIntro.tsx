import type { Category } from "@/types/listing";

const INTRO: Record<Category, string> = {
  veterinarian:
    "Your pet's care starts here. Routine checkups, vaccinations, diagnostics, and emergency care — find a vet who'll know your pet by name.",
  groomer:
    "From routine baths and nail trims to full breed cuts, a good groomer keeps your pet comfortable, healthy, and photo-ready.",
  boarder:
    "Safe overnight care for when you can't be home. Most require current vaccinations, which you can share from FetchFiles in seconds.",
  daycare:
    "Supervised play and socialization while you're at work. Look for size-separated groups and attentive staff ratios.",
  sitter:
    "In-home care for pets who stress out at boarding facilities. Ideal for senior pets, cats, and multi-pet households.",
  shelter:
    "Adopt, foster, or volunteer. Shelters and rescues across the metro are full of pets looking for their next home.",
  pet_hotel:
    "Premium overnight stays with enrichment built in — play yards, private suites, webcams, and concierge-level care.",
  dog_park:
    "Off-leash space to run, play, and socialize. Bring water, waste bags, and patience — and make sure vaccinations are current.",
  pet_cafe:
    "Bakeries and cafes that make treats for pets. Human-safe ingredients, dog-friendly recipes, and the occasional custom cake.",
  pet_memorial:
    "Thoughtful, pet-focused burial, cremation, and memorial services when it's time to say goodbye.",
};

export function CategoryIntro({ category }: { category: Category }) {
  return (
    <p className="mt-3 text-ink-700 max-w-2xl leading-relaxed">
      {INTRO[category]}
    </p>
  );
}
