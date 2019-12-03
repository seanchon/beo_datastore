from rest_framework import views
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response

from reference.reference_model.models import OriginFile


class OriginFileView(views.APIView):
    """
    File containing customer Meter data. A "file" must be provided in the
    payload.
    """

    parser_classes = [MultiPartParser]

    def put(self, request, filename, format=None):
        f = request.data["file"]
        # TODO: add additional file validation
        if f.content_type == "text/csv":
            load_origin_file = OriginFile()
            load_origin_file.file.save(filename, f, save=True)
            load_origin_file.owners.add(request.user)
            # TODO: async ingest meters
            # TODO: return link to asset in response
            return Response(status=204)
        else:
            raise UnsupportedMediaType("Upload must be a .csv file.")
